import os
import json
import sys
import threading
import time
import uuid

from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from schema    import Schema, And, Use
from logger    import log
from constants import SECRET_DATABASE_CREDENTIALS
from constants import DEFAULT_DATA_INTERVAL, DEFAULT_DATA_OVERLAP, DEFAULT_TICKER_INTERVAL
from constants import EXECUTION_STATUS_RUNNING, EXECUTION_STATUS_SUCCESS, EXECUTION_STATUS_FAILED
from constants import HEARTBEAT_SLEEPING, HEARTBEAT_INTERVAL
from context   import Context
from errors    import Error


def _get_secret_data(ctx: Context, project_id: str, secret_id: str, version_id: str = "latest"):

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = ctx.secrets_cli.access_secret_version(name=name)

    except BaseException as ex:
        if ex.code != 404:
            return None, Error(f"Exception getting secret '{secret_id}': {str(ex)}")
        return None, None

    result = response.payload.data.decode('UTF-8')
    return result, None


def _get_database_connection(ctx: Context, project_id):
    try:
        # obter credenciais do secret manager
        #
        payload, err = _get_secret_data(ctx, project_id, SECRET_DATABASE_CREDENTIALS)
        if err is not None:
            return None, err

        db_credentials = json.loads(payload)
        
        # estabelecer conexão
        #
        conn = ctx.database_cli.connect(
            database = db_credentials["database"],
            host     = db_credentials["host"],
            port     = db_credentials["port"],
            user     = db_credentials["user"],
            password = db_credentials["password"]
        )
        return conn, None
    
    except Exception as ex:
        return None, Error(f"Exception connectiong to PostgreSQL: {str(ex)}")


def _get_job_config(ctx: Context, conn, job_name: str):
    """
    CREATE TABLE ingestion_job_config 
    (
        job_name    VARCHAR(64) PRIMARY KEY,
        tickers     VARCHAR(1000) NOT NULL,
        interval    INTEGER,
        overlap     INTEGER,
        bucket_name VARCHAR(128) NOT NULL,
        updated_by  VARCHAR(64)  NOT NULL,
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
    CREATE CONSTRAINT unique_job_name UNIQUE (job_name);
    """    
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT 
                    job_name,
                    tickers,
                    interval,
                    overlap,
                    bucket_name,
                    updated_by,
                    created_at,
                    updated_at
                FROM 
                    ingestion_job_config
                WHERE 
                    job_name = '%s'
                AND
                    active = TRUE
            """
            cursor.execute(query, (job_name,))
            result = cursor.fetchone()

            return (dict(result), None) if result else None
        
    except Exception as ex:
        return None, Error(f"Exception getting job configuration from PostgreSQL: {str(ex)}")
    
    # TODO: verificar se há necessidade de rollback no cursor
    #


def _get_last_successful_execution(ctx: Context, conn, job_name: str):
    """
    CREATE TABLE ingestion_job_execution 
    (
        id                  SERIAL PRIMARY KEY,
        execution_id        VARCHAR(64) NOT NULL,
        job_name            VARCHAR(64) NOT NULL,
        tickers             VARCHAR(1000) NOT NULL,
        interval            INTEGER NOT NULL,
        overlap             INTEGER NOT NULL,
        bucket_name         VARCHAR(128) NOT NULL,
        start_time          TIMESTAMP NOT NULL,
        end_time            TIMESTAMP,
        filename            VARCHAR(128),
        records             INTEGER,
        status              VARCHAR(64) NOT NULL,
        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
    CREATE CONSTRAINT unique_execution_id UNIQUE (execution_id);
    """
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT 
                    * 
                FROM 
                    ingestion_job_execution 
                WHERE 
                    job_name = '%s' 
                AND 
                    status = 'SUCCESS' 
                ORDER BY 
                    end_time DESC 
                LIMIT 
                    1
            """
            cursor.execute(query, (job_name,))
            result = cursor.fetchone()

            return (dict(result), None) if result else None
        
    except Exception as ex:
        return None, Error(f"Exception getting last job execution from PostgreSQL: {str(ex)}")

    # TODO: verificar se há necessidade de rollback no cursor
    #


def _create_execution_record(ctx: Context, conn):
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO ingestion_job_execution 
                (
                    execution_id, 
                    job_name, 
                    tickers, 
                    interval, 
                    overlap,
                    bucket_name,
                    start_time,
                    status
                )
                VALUES 
                (
                    %s, -- execution_id
                    %s, -- job_name
                    %s, -- tickers
                    %s, -- interval
                    %s, -- overlap
                    %s, -- bucket_name
                    CURRENT_TIMESTAMP, 
                    %s
                )
                RETURNING id
            """
            #
            # ==== RUNNING ====
            #
            ctx.execution.status = EXECUTION_STATUS_RUNNING

            cursor.execute(query, (ctx.execution.execution_id, 
                                   ctx.execution.job_name, 
                                   ctx.execution.tickers, 
                                   ctx.execution.interval, 
                                   ctx.execution.overlap,
                                   ctx.execution.bucket_name))
            
            ctx.execution.id = cursor.fetchone()[0]

            conn.commit()
            return ctx.execution.id, None
        
    except Exception as ex:
        return None, Error(f"Exception creating execution record in PostgreSQL: {str(ex)}")

    # TODO: verificar se há necessidade de rollback no cursor
    #


def _update_execution_status(ctx: Context, conn, status: str):
    try:
        with conn.cursor() as cursor:

            query = """
            UPDATE 
                ingestion_job_execution 
            SET 
                filename   = COALESCE(%s, filename),
                records    = COALESCE(%s, records),
                status     = %s, 
                updated_at = CURRENT_TIMESTAMP
            WHERE 
                execution_id = %s
            """
            #
            # ==== STATUS ====
            #
            ctx.execution.status = status

            cursor.execute(query, (ctx.execution.filename if ctx.execution.filename is not None else 'NULL', 
                                   ctx.execution.records  if ctx.execution.records  is not None else 'NULL', 
                                   ctx.execution.status,
                                   ctx.execution.execution_id))
            conn.commit()
            return None

    except Exception as ex:
        return None, Error(f"Exception updating execution record in PostgreSQL: {str(ex)}")

    # TODO: verificar se há necessidade de rollback no cursor
    #


def _extract_stock_data(ctx: Context):
    log.info(f"Beggining tickers extraction")
    log.info(f"Extraction params \
                tickers:         {ctx.execution.tickers}, \
                start_time:      {ctx.execution.start_time}, \
                end_time:        {ctx.execution.end_time}, \
                ticker_interval: {DEFAULT_TICKER_INTERVAL}, \
                bucket:          {ctx.execution.bucket_name}")
    
    # data de execução para nomear os arquivos
    #
    execution_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # dataframe consolidado da extração
    #
    stock_df_columns = ["Date", "Ticker", "Open", "Adj Close", "Close", "High", "Low", "Volume"]
    stock_df = pd.DataFrame(columns=stock_df_columns)

    # extrair dados para cada ticker
    #
    for ticker in ctx.execution.tickers:
        try:
            log.info(f"Extracting ticker {ticker}")
            
            aux_df = yf.download(ticker, 
                                    start    = ctx.execution.start_time, 
                                    end      = ctx.execution.end_time, 
                                    interval = DEFAULT_TICKER_INTERVAL, 
                                    progress = False)

            if aux_df.empty:
                # TODO: criar método para envio de mensagem de log
                # para canal de contingência
                #
                log.warning(f"Empty dataframe for ticker '{ticker}'")
                continue
            
            # resetar index para transformar a data em coluna
            #
            aux_df = aux_df.reset_index()
            aux_df['Ticker'] = ticker
            aux_df = aux_df[stock_df_columns]

            stock_df = pd.concat([stock_df, aux_df], ignore_index=True)
            
        except Exception as ex:
            return None, Error(f"Error extracting data ticker {ticker}: {str(ex)}")
    
    return stock_df, None


def _save_dataframe_on_gcs(ctx: Context, df: pd.DataFrame):

    # as bibliotecas fsspec e gcsfs permitem salvar o dataframe 
    # diretamente no GCS
    #
    bucket_name = ctx.execution.bucket_name
    index_name  = ctx.execution.execution_id
    file_name   = f"raw_{ctx.execution.execution_id}"

    # utilizando hive partitioning
    #
    filepath = f"gs://{bucket_name}/id={index_name}/{file_name}.parquet"

    try:
        df.to_parquet(filepath, index=False)
    
    except Exception as ex:
        return None, Error(f"Error saving extraction on storage {filepath}: {str(ex)}", 500)
    
    return filepath, None


def _heartbeat(ctx: Context, conn, mutex: threading.Lock):
    log.info(f"Heartbeat for {ctx.execution.execution_id} initiated")

    tms = datetime.now()

    while True:
        # aguarda por um pequeno intervalo, para evitar 
        # que a conclusão da execução seja afetada pelo
        # tempo do heartbeat
        #
        time.sleep(HEARTBEAT_SLEEPING)

        # verificar se já é hora de atualizar  o status 
        # da execução
        #
        if tms + HEARTBEAT_INTERVAL < datetime.now():
            try:
                # tenho que ter exclusividade no controlador
                # de contexto para garantir que ninguém esteja
                # manipulando o status
                #
                mutex.acquire()

                if ctx.execution.status == EXECUTION_STATUS_RUNNING:

                    # se estiver em execução, atualizar o status e a 
                    # coluna 'updated_at'
                    #
                    err = _update_execution_status(ctx, conn, EXECUTION_STATUS_RUNNING)
                    if err is not None:
                        Error(f"Error updating execution status on hearbeath thread")

                        # TODO: avaliar a conveniência de fechar a conexão
                        # do banco de dados para forçar o reporte do erro 
                        # na thread principal.
                        #
                        if conn:
                            conn.close()

                        continue

                    tms = datetime.now()

                else:
                    # se o status é diferente de 'running', significa que 
                    # a execução foi concluida
                    #
                    break

            except Exception as ex:
                # TODO: adicionar a stack trace no log de erro
                #
                Error(f"Exception on heartbeat thread for {ctx.execution.execution_id}: {str(ex)}", 500)

            finally:
                mutex.release()

    log.info(f"Heartbeat for {ctx.execution.execution_id} finalized")


def _ingestion(ctx: Context, params: dict) -> Error | None:
    try:
        mutex = threading.Lock()

        project_id   = params['project_id']
        job_name     = params['job_name']
        execution_id = params['execution_id']
        
        # estabelecer conexão com o banco
        #
        conn, err = _get_database_connection(ctx, project_id)
        if err is not None:
            return err

        log.debug(f"Database connection established")

        # buscar configuração do job
        #
        job_config, err = _get_job_config(ctx, conn, job_name)
        if err is not None:
            return err
        
        # TODO: avaliar se quanto o job estiver inativo, deve ser 
        # retornado um erro ou um sucesso
        #
        if not job_config:
            return None, Error(f"Job {job_name} not found or inactive")
       
        log.debug(f"Job config loaded: {str(job_config)}")

        # verificar execução anterior (opcional - para continuidade)
        #
        last_execution, err = _get_last_successful_execution(ctx, conn, job_name)
        if err is not None:
            return err

        log.debug(f"Last successful execution: {last_execution if last_execution else 'None'}")
        
        # composição dos dados da execução
        # TODO: extrair para uma função
        #
        ctx.execution.id           = None
        ctx.execution.execution_id = execution_id
        ctx.execution.job_name     = job_name
        ctx.execution.tickers      = job_config['tickers']
        ctx.execution.interval     = job_config['interval'] if 'interval' in job_config else DEFAULT_DATA_INTERVAL
        ctx.execution.overlap      = job_config['overlap']  if 'overlap'  in job_config else DEFAULT_DATA_OVERLAP
        ctx.execution.bucket_name  = job_config['bucket_name']
        ctx.execution.start_time   = last_execution["end_time"] if last_execution else None
        ctx.execution.start_time  -= timedelta(seconds=ctx.execution.overlap) if ctx.execution.start_time else None
        ctx.execution.end_time     = datetime.now()

        # criar registro de execução
        #
        id, err = _create_execution_record(ctx, conn)
        if err is not None:
            return err

        ctx.executuion.id = id
        log.debug(f"Execution record created with id: {ctx.execution.id}")

        # cria a threa de heartbeat
        #
        heartbeat_thread = threading.Thread(target=_heartbeat, args=(ctx, conn, mutex))
        heartbeat_thread.start()

        # executar a ingestão
        #
        stock_df, err = _extract_stock_data(ctx)
        if err is not None:
            _update_execution_status(ctx, conn, EXECUTION_STATUS_FAILED)
            return err

        ctx.execution.records = len(stock_df)
        log.debug(f"Extraction for execution {ctx.execution.id} completed with {ctx.execution.records} records")

        # salvar dataframe no storage
        #
        filepath, err = _save_dataframe_on_gcs(ctx, stock_df)
        if err is not None:
            _update_execution_status(ctx, conn, EXECUTION_STATUS_FAILED)
            return err

        ctx.execution.filename = filepath
        log.debug(f"Dataframe saved on storage {ctx.execution.filename}")

        # atualizar o status da execução em sucesso
        #
        err = _update_execution_status(ctx, conn, EXECUTION_STATUS_SUCCESS)
        if err is not None:
            return err

        # aguarda a finalização da thread de heartbeat
        #         
        heartbeat_thread.join()

        return None
        
    except Exception as ex:
        # TODO: adicionar a stack trace no log de erro
        #
        return Error(f"Error ingesting data: {str(ex)}", 500)
    
    finally:
        if conn:
            conn.close()


def settings(ctx: Context, body: dict = None) -> tuple[dict, Error | None]:

    params = body
    schema = Schema({
        "project_id":   And(str, Use(str.lower)),
        "job_name":     And(str, Use(str.lower)),
        "execution_id": And(str, Use(str.lower)),
    })

    if schema.is_valid(params) == False:
        return None, Error("Requisição sem corpo JSON válido", 400)

    return params, None


def execute(ctx: Context, params: dict) -> tuple[str, Error | None]:

    err = _ingestion(ctx, params)
    if err is not None:
        return None, err

    return {
        "status":       "success",
        "execution_id": ctx.execution.execution_id,
        "ingestion_id": ctx.execution.id,
        "records":      ctx.execution.records,
        "filename":     ctx.execution.filename
    }, None
