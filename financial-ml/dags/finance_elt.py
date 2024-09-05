import logging
import os
from sys import prefix

from pendulum import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
from airflow.decorators import dag, task, task_group
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator


dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv()

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


AZURE_CONTAINER_NAME=os.environ.get('AZURE_CONTAINER_NAME')
AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW='delme2'
POKE_INTERVAL = 1 * 10


default_args = {
    'owner': 'me',
    'execution_timeout':timedelta(seconds=65),
}

@dag(
    start_date=datetime(2024, 9, 4),
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['financial-ml'],
)
def finance_elt():
    @task_group(group_id='wait_for_blobs')
    def wait_for_blobs():
        # Define sensor tasks within the task group
        wait_for_satisfaction=WasbPrefixSensor(
            task_id="wait_for_blobs_charge",
            wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW,
            container_name=AZURE_CONTAINER_NAME,
            poke_interval=POKE_INTERVAL,
            timeout=1 * 30,
            mode='reschedule',
            prefix='charge/'
        )

        wait_for_charge=WasbPrefixSensor(
            task_id="wait_for_blobs_satisfaction",
            wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW,
            container_name=AZURE_CONTAINER_NAME,
            poke_interval=POKE_INTERVAL,
            timeout=1 * 30,
            mode='reschedule',
            prefix='satisfaction/'
        )

        # Not sure if we need this !!!
        return [wait_for_charge, wait_for_satisfaction]

    def get_blob_names_from_container(blob_name: str, **kwargs) -> list[str]:
        hook = WasbHook(wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW)
        blob_service_client = hook.blob_service_client

        if blob_service_client is None:
            raise ValueError("Blob service client is not initialized.")

        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        blobs = container_client.list_blobs(name_starts_with=blob_name)
        blob_names = [blob.name for blob in blobs]
        return blob_names

    # TaskGroup to wait for both 'charge' and 'satisfaction' blobs
    ingest_done = wait_for_blobs()
    # ingest_done

    # charge_data = PythonOperator(
    #     task_id='get_charge_data',
    #     provide_context=True,
    #     python_callable=get_blob_names_from_container,
    #     op_kwargs={'blob_name': 'charge/'}
    # )

    # ingest_done >> charge_data

# TRU THIS  https://sihan.hashnode.dev/how-to-use-postreshook-in-airflow

# PostgresHook and copy_expert -  https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py#L117
# sql for hook - https://www.projectpro.io/recipes/extract-data-from-postgres-and-store-it-into-csv-file-by-executing-python-task-airflow-dag
# Apache Airflow & PostgreSQL -  https://medium.com/@sriskandaryan/apache-airflow-postgresql-5f416f841da4
finance_elt()