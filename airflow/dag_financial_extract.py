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

CREDENTIAL                   = "/home/airflow/gcs/data/gcp_conn.json"
CLOUD_FUNCTION_CONNECTION_ID = "financial_extract_gcp_http_cf"
CLOUD_FUNCTION_ENDPOINT      = "financial_extract"
TASK_ID_EXTRACT              = "financial_extract"

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

    print(f"==> Task {execution_id}: financial_extract - initialized")

    # Autenticação
    #
    connection_id = BaseHook.get_connection(CLOUD_FUNCTION_CONNECTION_ID)
    audience = f"{connection_id.host}/{CLOUD_FUNCTION_ENDPOINT}"
    request  = google.auth.transport.requests.Request()
    token    = google.oauth2.id_token.fetch_id_token(request, audience)

    headers = {
        "Authorization": f"bearer {token}", 
        "Content-Type":  "application/json"
    }

    params = {
        "execution_id": execution_id
    }

    print(f"==> Task {execution_id}: params {params}")

    invoke_request = SimpleHttpOperator(
        task_id             = TASK_ID_EXTRACT,
        method              = 'POST',
        http_conn_id        = CLOUD_FUNCTION_CONNECTION_ID,
        endpoint            = CLOUD_FUNCTION_ENDPOINT,
        headers             = headers,
        data                = json.dumps(params),
        on_failure_callback = on_failure_callback_fn,
        response_check      = lambda response: response.status_code == 200,
    )
    invoke_request.execute(dict())

    print(f"==> Task {execution_id}: finalized")


# ------------------------------------------------------------------------------
#   DAG
#

DAG_ID       = 'raw_financial_data'
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

    task_extract
