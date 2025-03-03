import json
import logging
import os
import uuid

from datetime import datetime, timedelta

import google.auth.transport.requests
import google.oauth2.id_token

from airflow                                         import DAG
from airflow.hooks.base_hook                         import BaseHook
from airflow.models                                  import Variable
from airflow.models.baseoperator                     import BaseOperator
from airflow.operators.http_operator                 import SimpleHttpOperator
from airflow.operators.python_operator               import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates                             import days_ago

CREDENTIAL            = "/home/airflow/gcs/data/gcp_conn.json"

CF_CONNECTION_ID      = "gcp_http_cf"
CF_ENDPOINT_EXTRACT   = "financial_extract"
CF_ENDPOINT_TRANSFORM = "financial_transform"
CF_ENDPOINT_AGGREGATE = "financial_aggregate"

TASK_ID_EXTRACT       = "financial_extract"
TASK_ID_TRANSFORM     = "financial_transform"
TASK_ID_AGGREGATE     = "financial_aggregate"

XCOM_EXTRACT          = "financial_extract_xcom"
XCOM_TRANSFORM        = "financial_transform_xcom"

LOGGER = logging.getLogger("airflow.task")


def on_failure_callback_fn(context):

    LOGGER.warn(f"Callback executing...")

    # As variáveis de ambiente aqui obtidas devem ser equivalentes ao código 
    # contido nas propriedades do módulo pipeline_config: self.slack_channel
    # e self.slack_username
    #
    channel  = Variable.get('SLACK_ETL_ALERT_CHANNEL')
    username = Variable.get('SLACK_ETL_ALERT_USERNAME')
    title    = "Failed Airflow Job"
    body = {
        "dag":       context["dag"].dag_id, 
        "task":      context["task"].task_id,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        #"callstack": str(traceback.print_stack())
    }

    execution_id = context["execution_id"] if "execution_id" in context else None
    if execution_id is not None:
        body["execution_id"] = execution_id

    color = "danger"

    ex = context["exception"] if "exception" in context else None
    if ex is not None:
        LOGGER.error(f"{str(ex)}")

    LOGGER.info(f"ALERT: {channel} - {username}")

    alert_operator(channel, username, color, title, body).execute(context)
    LOGGER.warn(f"callback executed")


def alert_operator(channel: str, username: str, color: str, title: str, msg_body: dict) -> BaseOperator:

    return  SlackWebhookOperator(
        task_id               = 'failure',
        slack_webhook_conn_id = SLACK_WEBHOOK_CONN_ID,
        message               = msg_body,
        blocks = [{
            "type":     "divider",
            "block_id": "divider1"
        },{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{title}*"
            }
        }],
        attachments = [{
            "mrkdwn_in": ["text"],
            'color':     color,
            'text':     f'```{json.dumps(msg_body, indent=4)}```'
        }]
    )


def financial_extract(ti, **kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIAL

    execution_id = str(uuid.uuid4())

    print(f"==> Task extraction {execution_id}: initialized")

    # Autenticação
    #
    connection_id = BaseHook.get_connection(CF_CONNECTION_ID)
    audience = f"{connection_id.host}/{CF_ENDPOINT_EXTRACT}"
    request  = google.auth.transport.requests.Request()
    token    = google.oauth2.id_token.fetch_id_token(request, audience)

    headers = {
        "Authorization": f"bearer {token}", 
        "Content-Type":  "application/json"
    }

    params = {
        "project_id":   kwargs["project_id"],
        "job_name":     kwargs["job_name"],
        "execution_id": execution_id
    }

    print(f"==> Task extraction {execution_id}: params {params}")

    invoke_request = SimpleHttpOperator(
        task_id             = TASK_ID_EXTRACT,
        method              = 'POST',
        http_conn_id        = CF_CONNECTION_ID,
        endpoint            = CF_ENDPOINT_EXTRACT,
        headers             = headers,
        data                = json.dumps(params),
        on_failure_callback = on_failure_callback_fn,
        response_check      = lambda response: response.status_code == 200,
    )
    invoke_request.execute(dict())

    response = ti.xcom_pull(task_ids=TASK_ID_EXTRACT)

    try:
        response_body = json.loads(response)
    except:
        response_body = {}

    if response.status_code != 200:
        raise ValueError(f"Exception executing extraction: {response_body}")
    if "status" in response_body and response_body.status_code != 200:
        raise ValueError(f"Error executing extraction: {response_body}")
    
    ti.xcom_push(key=XCOM_EXTRACT, value=response_body)
    print(f"==> Task extraction {execution_id}: finalized")


def financial_transform(ti, **kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIAL

    params = ti.xcom_pull(key=XCOM_EXTRACT, task_ids=TASK_ID_EXTRACT)
    execution_id = params[0]['execution_id']
    filename     = params[0]['filename']

    print(f"==> Task transform {execution_id}: initialized")

    # Autenticação
    #
    connection_id = BaseHook.get_connection(CF_CONNECTION_ID)
    audience = f"{connection_id.host}/{CF_ENDPOINT_TRANSFORM}"
    request  = google.auth.transport.requests.Request()
    token    = google.oauth2.id_token.fetch_id_token(request, audience)

    headers = {
        "Authorization": f"bearer {token}", 
        "Content-Type":  "application/json"
    }

    params = {
        "project_id":   kwargs["project_id"],
        "job_name":     kwargs["job_name"],
        "execution_id": execution_id,
        "filename":     filename,
    }

    print(f"==> Task transform {execution_id}: params {params}")

    invoke_request = SimpleHttpOperator(
        task_id             = TASK_ID_EXTRACT,
        method              = 'POST',
        http_conn_id        = CF_CONNECTION_ID,
        endpoint            = CF_ENDPOINT_TRANSFORM,
        headers             = headers,
        data                = json.dumps(params),
        on_failure_callback = on_failure_callback_fn,
        response_check      = lambda response: response.status_code == 200,
    )
    invoke_request.execute(dict())

    response = ti.xcom_pull(task_ids=TASK_ID_EXTRACT)

    try:
        response_body = json.loads(response)
    except:
        response_body = {}

    if response.status_code != 200:
        raise ValueError(f"Exception executing transform: {response_body}")
    if "status" in response_body and response_body.status_code != 200:
        raise ValueError(f"Error executing transform: {response_body}")
    
    ti.xcom_push(key=XCOM_TRANSFORM, value=response_body)
    print(f"==> Task transform {execution_id}: finalized")


def financial_aggregate(ti, **kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIAL

    params = ti.xcom_pull(key=XCOM_TRANSFORM, task_ids=TASK_ID_TRANSFORM)
    execution_id = params[0]['execution_id']
    filename     = params[0]['filename']

    print(f"==> Task aggregate {execution_id}: initialized")

    # Autenticação
    #
    connection_id = BaseHook.get_connection(CF_CONNECTION_ID)
    audience = f"{connection_id.host}/{CF_ENDPOINT_AGGREGATE}"
    request  = google.auth.transport.requests.Request()
    token    = google.oauth2.id_token.fetch_id_token(request, audience)

    headers = {
        "Authorization": f"bearer {token}", 
        "Content-Type":  "application/json"
    }

    params = {
        "project_id":   kwargs["project_id"],
        "job_name":     kwargs["job_name"],
        "execution_id": execution_id,
        "filename":     filename,
    }

    print(f"==> Task aggregate {execution_id}: params {params}")

    invoke_request = SimpleHttpOperator(
        task_id             = TASK_ID_AGGREGATE,
        method              = 'POST',
        http_conn_id        = CF_CONNECTION_ID,
        endpoint            = CF_ENDPOINT_AGGREGATE,
        headers             = headers,
        data                = json.dumps(params),
        on_failure_callback = on_failure_callback_fn,
        response_check      = lambda response: response.status_code == 200,
    )
    invoke_request.execute(dict())

    response = ti.xcom_pull(task_ids=TASK_ID_AGGREGATE)

    try:
        response_body = json.loads(response)
    except:
        response_body = {}

    if response.status_code != 200:
        raise ValueError(f"Exception executing aggregate: {response_body}")
    if "status" in response_body and response_body.status_code != 200:
        raise ValueError(f"Error executing aggregate: {response_body}")
    
    ti.xcom_push(key=XCOM_TRANSFORM, value=response_body)
    print(f"==> Task aggregate {execution_id}: finalized")


# ------------------------------------------------------------------------------
#   DAG
#

DAG_ID       = 'financial_data'
SCHEDULE     = '5 */1 * * *'
DEFAULT_ARGS = {
    'owner':            'airflow',
    'depends_on_past':  False,
    'email':            [],
    'email_on_failure': False,
    'email_on_retry':   False,
    'catchup':          False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=2)
}

with DAG(
    dag_id            = DAG_ID,
    schedule_interval = SCHEDULE,
    start_date        = days_ago(1),
    default_args      = DEFAULT_ARGS,
    tags              = ['cloud_function', 'http'],
) as dag:

    task_extract = PythonOperator(
        task_id         = TASK_ID_EXTRACT,
        python_callable = financial_extract,
        provide_context = True,
    )

    task_transform = PythonOperator(
        task_id         = TASK_ID_EXTRACT,
        python_callable = financial_transform,
        provide_context = True,
    )

    task_aggregate = PythonOperator(
        task_id         = TASK_ID_AGGREGATE,
        python_callable = financial_aggregate,
        provide_context = True,
    )

    task_extract >> task_transform
