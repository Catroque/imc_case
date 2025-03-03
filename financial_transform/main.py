import os
import pandas as pd

import flask
import functions_framework
import psycopg2

from google.cloud import storage
from google.cloud import secretmanager

from logger  import log, filter
from context import Context
from errors  import Error
from metrics import Metrics
from service import execute, settings


def _entrypoint(ctx: Context, request: flask.Request) -> tuple[str, Error | None]:
    log.info(f"Process initializing...")
    
    body = request.get_json()

    try:
        params, err = settings(ctx, body)
        if err is not None:
            return None, err

    except BaseException as ex:
        err = Error("Exception loading settings: {}".format(str(ex)), 500)
        return None, err

    try:
        execution_id = params["execution_id"]
        filter.set_execution_id(execution_id)

        log.info(f"Process initialized")

    except BaseException as ex:
        err = Error("Exception writing settings: {}".format(str(ex)), 500)
        return None, err

    try:
        msg, err = execute(ctx, params)
        if err is not None:
            return None, err

    except BaseException as ex:
        err = Error("Exception processing request: {}".format(str(ex)), 500)
        return None, err

    log.info("Process completed successfully")
    return msg, None


@functions_framework.http
def main(request: flask.Request) -> tuple[str, int]:

    ctx = Context()
    ctx.metrics_cli  = Metrics()
    ctx.secrets_cli  = secretmanager.SecretManagerServiceClient()
    ctx.storage_cli  = storage.Client()
    ctx.database_cli = psycopg2

    message = None
    status  = 200

    message, err = _entrypoint(ctx, request)
    if err is not None:
        message = {
            "status":       "error",
            "execution_id": ctx.execution.execution_id,
            "message":      err.Message(),
        }
        status  = err.code
    
    ctx.metrics_cli.registry("cloud_function")

    return message, status


if __name__ == '__main__':

    from types import SimpleNamespace

    payload = {
        'project_id':   "9876543210",
        'job_name':     "financial_data",
        'execution_id': "9d8c348f-41e2-4224-84ea-4da608b1eec5",
    }

    request = SimpleNamespace(
        get_json=lambda : payload
    )

    err = main(request)
    if err is not None:
        sys.exit(-1)