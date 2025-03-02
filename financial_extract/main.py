import sys
import uuid

from unittest import mock

import flask
import functions_framework
import psycopg2

from google.cloud import storage
from google.cloud import secretmanager

import constants

from logger  import log, filter
from context import Context
from errors  import Error
from metrics import Metrics
from mocks   import SecretsMock, StorageMock, DatabaseMock
from service import execute, settings


def _entrypoint(ctx: Context, request: flask.Request) -> Error | None:
    log.info(f"Process initializing...")
    
    body = request.get_json()

    try:
        params, err = settings(ctx, body)
        if err is not None:
            return err

    except BaseException as ex:
        err = Error("Exception loading settings: {}".format(str(ex)), 500)
        return err

    try:
        execution_id = params["execution_id"]
        filter.set_execution_id(execution_id)

        log.info(f"Process initialized")

    except BaseException as ex:
        err = Error("Exception writing settings: {}".format(str(ex)), 500)
        return err

    try:
        err = execute(ctx, params)
        if err is not None:
            return err

    except BaseException as ex:
        err = Error("Exception processing request: {}".format(str(ex)), 500)
        return err

    log.info("Process completed successfully")
    return None


@functions_framework.http
def main(request: flask.Request) -> tuple[str, int]:

    ctx = Context()
    ctx.metrics_cli  = Metrics()
    ctx.secrets_cli  = secretmanager.SecretManagerServiceClient()
    ctx.storage_cli  = storage.Client()
    ctx.database_cli = psycopg2

    message = "success"
    status  = 200

    err = _entrypoint(ctx, request)
    if err is not None:
        message = err.Message()
        status  = err.code

    ctx.metrics_cli.registry("cloud_function")

    return ctx.execution.execution_id, message, status


if __name__ == '__main__':

    secrets_mock = mock.Mock(spec=SecretsMock)
    secrets_mock.access_secret_version.return_value = secretmanager.AccessSecretVersionResponse(name=f"projects/9876543210/secrets/{constants.SECRET_DATABASE_CREDENTIALS}/versions/latest", 
            payload=secretmanager.SecretPayload(data=b'{"host": "127.0.0.1", "database": "postgres", "user": "postgres", "password": "postgres", "port": "5432"}'))

    mock.patch.object(psycopg2, "connect", new_callable=lambda *args, **kwargs: None)

    ctx = Context()
    ctx.metrics_cli  = Metrics()
    ctx.secrets_cli  = secrets_mock
    ctx.storage_cli  = StorageMock()
    ctx.database_cli = psycopg2

    from types import SimpleNamespace

    payload = {
        'project_id':   "9876543210",     # sys.argv[1],
        'job_name':     "financial_data", # sys.argv[2],
        'execution_id': str(uuid.uuid4()) # sys.argv[3],
    }

    request = SimpleNamespace(
        get_json=lambda : payload
    )

    err = _entrypoint(ctx, request)
    if err is not None:
        sys.exit(-1)