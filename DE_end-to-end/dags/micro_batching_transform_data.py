import os
import time

import pendulum
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)


def get_azure_credentials_from_azure_conn_id(conn_id: str) -> tuple:
    connection = BaseHook.get_connection(conn_id)
    login = connection.login
    password = connection.password
    return (login, password)

def get_postgres_credentials_from_conn_id(conn_id: str) -> tuple:
    connection = BaseHook.get_connection(conn_id)
    login = connection.login
    password = connection.password
    host = connection.host
    db_name = connection.schema
    port = f'{connection.port}'

    login = 'airflow' if '*' in login else login
    password = 'airflow' if '*' in password else password

    return (login, password, host, port, db_name)


AZURE_BLOB_STORAGE_CONN: str ='delme-storage-account'
AZURE_CONTAINER_NAME: str = os.environ.get("MICRO_BATCHING_AZURE_CONTAINER_NAME")
(AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY) = get_azure_credentials_from_azure_conn_id(AZURE_BLOB_STORAGE_CONN)

POSTGRES_CONN_ID: str = "delme-postgresql"
POSTGRES_TABLE: str = os.environ.get("MICRO_BATCHING_POSTGRES_TABLE")
(POSTGRES_LOGIN, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME) = get_postgres_credentials_from_conn_id(POSTGRES_CONN_ID)


default_args = {
    'owner': 'delme',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


@dag(
    start_date=datetime(2024, 9, 16),
    schedule='@daily',
    catchup=False,
    tags=['micro-batching', 'load-data', 'delme']
)
def micro_batching_transform_data():

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    # Define application arguments
    application_args = [
        # Section Azure
        "--AZURE_CONTAINER_NAME", AZURE_CONTAINER_NAME,
        "--AZURE_STORAGE_ACCOUNT_NAME", AZURE_STORAGE_ACCOUNT_NAME,
        "--AZURE_STORAGE_ACCOUNT_KEY", AZURE_STORAGE_ACCOUNT_KEY,
        # Section Postgres
        "--POSTGRES_TABLE", POSTGRES_TABLE,
        "--POSTGRES_LOGIN", POSTGRES_LOGIN,
        "--POSTGRES_PASSWORD", POSTGRES_PASSWORD,
        "--POSTGRES_HOST", POSTGRES_HOST,
        "--POSTGRES_PORT", POSTGRES_PORT,
        "--POSTGRES_DBNAME", POSTGRES_DBNAME,
    ]

    micro_batching_prepare_data_for_transform_job = SparkSubmitOperator(
        task_id="micro_batching_prepare_data_for_transform_job",
        conn_id="delme-pyspark",
        application="jobs/micro_batching_prepare_data_for_transform_job.py",
        packages="org.postgresql:postgresql:42.7.3",
        application_args=application_args
    )

    start >> micro_batching_prepare_data_for_transform_job >> finish


micro_batching_transform_data()