import logging
import os

from pendulum import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from azure.storage.blob import BlobServiceClient
from azure.storage.blob._models import PublicAccess


dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)


AZURE_CONTAINER_NAME: str = os.environ.get("MICRO_BATCHING_AZURE_CONTAINER_NAME")
AZURE_BLOB_STORAGE_CONN: str ='delme-storage-account'
POSTGRES_TABLE: str = os.environ.get("MICRO_BATCHING_POSTGRES_TABLE")
POSTGRES_CONN_ID: str = "delme-postgresql"


def get_azure_conn_string_from_conn_id(conn_id: str) -> str:
    connection = BaseHook.get_connection(conn_id)
    extra = connection.extra_dejson
    connection_string = extra.get('connection_string', '')
    return connection_string


def get_azure_blob_connection(conn_id: str) -> Optional[BlobServiceClient]:
    try:
        blob_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(conn_id)
        logging.info("Azure Container Client exists")
        return blob_client
    except Exception as ex:
        logging.error(f"Connection failed: {str(ex)}")
        return None


def create_container(blob_client: Optional[BlobServiceClient], container_name: str) -> bool:
    try:
        containers = blob_client.list_containers()
        if container_name not in containers:
            blob_client.create_container(container_name, public_access=PublicAccess.CONTAINER)
            logging.info(f"Container '{container_name}' created'.")
        return True
    except Exception as ex:
        logging.error(f"Error with creating container '{container_name}': {str(ex)}")
        return False

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
    tags=['micro-batching', 'prepare-env', 'delme']
)
def micro_batching_prepare_env():
    @task_group(group_id='prepare_azure_env')
    def prepare_azure_env() -> None:
        @task
        def create_container(blob_client: Optional[BlobServiceClient], container_name: str) -> bool:
            print(f"------------------  {POSTGRES_TABLE}    ------------")
            try:
                containers =[ container.name for container in blob_client.list_containers()]
                if container_name not in containers:
                    blob_client.create_container(container_name, public_access=PublicAccess.CONTAINER)
                    logging.info(f"Container '{container_name}' created'.")
                return True
            except Exception as ex:
                logging.error(f"Error with creating container '{container_name}': {str(ex)}")
                return False

        conn_string = get_azure_conn_string_from_conn_id(AZURE_BLOB_STORAGE_CONN)
        blob_service_client: Optional[BlobServiceClient] = get_azure_blob_connection(conn_string)

        container_tasks = [
            create_container.override(task_id=f"create_container_{AZURE_CONTAINER_NAME}")(blob_service_client, AZURE_CONTAINER_NAME),
            create_container.override(task_id=f"create_container_checkpoint")(blob_service_client, 'checkpoint')
        ]

        blob_service_client.close()


    start = EmptyOperator(task_id='start')

    create_logs_table = PostgresOperator(
        task_id=f"create_logs_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            
            CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
                username VARCHAR(255),
                age INT,
                gender VARCHAR(50),
                ad_position VARCHAR(255),
                browsing_history TEXT,
                activity_time TIMESTAMP,
                ip_address VARCHAR(45),
                log TEXT,
                redirect_from VARCHAR(255),
                redirect_to VARCHAR(255),
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                created_at TIMESTAMP DEFAULT NOW(),
                status VARCHAR(50) DEFAULT 'uploaded'
            );
        """,
        params={'table_name': POSTGRES_TABLE}
    )

    trigger_generate_data = TriggerDagRunOperator(
        task_id="trigger_generate_data",
        trigger_dag_id="micro_batching_generate_data",
        conf={"message": "Message to pass to Generate Data Dag."},
    )

    start >> [prepare_azure_env(), create_logs_table] >> trigger_generate_data

micro_batching_prepare_env()