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

KAFKA_DATA_PRODUCE = 50
KAFKA_BOOTSTRAP_SERVER = os.environ.get("MICRO_BATCHING_KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.environ.get("MICRO_BATCHING_KAFKA_TOPIC")


default_args = {
    'owner': 'delme',
    'execution_timeout':timedelta(seconds=65),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


@dag(
    start_date=datetime(2024, 9, 13),
    schedule='@daily',
    catchup=False,
    tags=['micro-batching', 'load-data', 'delme']
)
def micro_batching_load_data():

    start = EmptyOperator(task_id='start')

    # Define application arguments
    application_args = [
        "--KAFKA_BOOTSTRAP_SERVER", KAFKA_BOOTSTRAP_SERVER,
        "--KAFKA_TOPIC", KAFKA_TOPIC,
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

    trigger_pyspark_job = SparkSubmitOperator(
        task_id="trigger_pyspark_job",
        conn_id="delme-pyspark",
        application="jobs/micro_batching_load_job.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.27.1",
        application_args=application_args
    )

    wait_for_one_minute = TimeDeltaSensor(
        task_id="wait_for_one_minute",
        # sleep_duration=timedelta(seconds=20),
        mode='reschedule',
        # delta=pendulum.duration(seconds=20)
        # delta=timedelta(minutes=1)
        delta=timedelta(seconds=30)
    )

    trigger_transform_data = TriggerDagRunOperator(
        task_id='trigger_transform_data',
        trigger_dag_id="micro_batching_transform_data",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start >> trigger_pyspark_job
    start >> wait_for_one_minute >> trigger_transform_data


micro_batching_load_data()