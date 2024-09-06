import logging
import os

from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
from airflow.decorators import dag, task, task_group
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator, get_current_context

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
    tags=['financial-ml', 'elt', 'delme'],
)
def finance_elt():

    start = EmptyOperator(
        task_id='start',
    )

    @task_group(group_id='phase_1_wait_for_blobs')
    def phase_1_wait_for_blobs():
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

        [wait_for_charge, wait_for_satisfaction]

    @task_group(group_id='phase_2_get_blob_names')
    def phase_2_get_blob_names():
        def get_blob_names_from_container(blob_name: str, **kwargs) -> list[str]:
            hook = WasbHook(wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW)
            blob_service_client = hook.blob_service_client

            if blob_service_client is None:
                raise ValueError("Blob service client is not initialized.")

            container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=blob_name)
            blob_names = [blob.name for blob in blobs]
            return blob_names

        @task(task_id="get_blob_names_for_charge")
        def get_blob_names_for_charge(blob_name: str):
            return get_blob_names_from_container(blob_name)

        @task(task_id="get_blob_names_for_satisfaction")
        def get_blob_names_for_satisfaction(blob_name: str):
            return get_blob_names_from_container(blob_name)

        @task(task_id="save_filepath_by_chunks")
        def save_filepath_by_chunks():
            ti = get_current_context()['ti']
            charge_data = ti.xcom_pull(task_ids='phase_2_get_blob_names.get_blob_names_for_charge')
            satisfaction_data = ti.xcom_pull(task_ids='phase_2_get_blob_names.get_blob_names_for_satisfaction')

        [get_blob_names_for_charge('charge/'), get_blob_names_for_satisfaction('satisfaction/')] >> save_filepath_by_chunks()

    @task(task_id="finish")
    def finish():
        # get from xcom
        logging.warning(f'FINISHHHHHHHHHH ')

    start >> phase_1_wait_for_blobs() >> phase_2_get_blob_names() >> finish()




    # charge_data = PythonOperator(
    #     task_id='get_charge_data',
    #     provide_context=True,
    #     python_callable=get_blob_names_from_container,
    #     op_kwargs={'blob_name': 'charge/'}
    # )

# TRU THIS  https://sihan.hashnode.dev/how-to-use-postreshook-in-airflow

# PostgresHook and copy_expert -  https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py#L117
# sql for hook - https://www.projectpro.io/recipes/extract-data-from-postgres-and-store-it-into-csv-file-by-executing-python-task-airflow-dag
# Apache Airflow & PostgreSQL -  https://medium.com/@sriskandaryan/apache-airflow-postgresql-5f416f841da4
finance_elt()