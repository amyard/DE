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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.db import provide_session
from airflow import DAG
from airflow.models import XCom


dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv()

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


AZURE_CONTAINER_NAME=os.environ.get('AZURE_CONTAINER_NAME')
AZURE_STORAGE_ACCOUNT_NAME=os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW='delme-storage-account'
POKE_INTERVAL = 1 * 10
POSTGRES_CONN_ID = "delme-postgresql"


@provide_session
def cleanup_xcom(task_id, session=None, **context):
    # https://stackoverflow.com/questions/46707132/how-to-delete-xcom-objects-once-the-dag-finishes-its-run-in-airflow
    dag = context["dag"]
    dag_id = dag._dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.task_id==task_id).delete()

default_args = {
    'owner': 'me',
    'execution_timeout':timedelta(seconds=65),
}

@dag(
    start_date=datetime(2024, 9, 4),
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    on_success_callback=cleanup_xcom,
    tags=['financial-ml', 'elt', 'delme'],
)
def finance_elt():
    # @task_group(group_id='phase_1_wait_for_blobs')
    # def phase_1_wait_for_blobs():
    #     wait_for_satisfaction=WasbPrefixSensor(
    #         task_id="wait_for_blobs_charge",
    #         wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW,
    #         container_name=AZURE_CONTAINER_NAME,
    #         poke_interval=POKE_INTERVAL,
    #         timeout=1 * 30,
    #         mode='reschedule',
    #         prefix='charge/'
    #     )
    #
    #     wait_for_charge=WasbPrefixSensor(
    #         task_id="wait_for_blobs_satisfaction",
    #         wasb_conn_id=AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW,
    #         container_name=AZURE_CONTAINER_NAME,
    #         poke_interval=POKE_INTERVAL,
    #         timeout=1 * 30,
    #         mode='reschedule',
    #         prefix='satisfaction/'
    #     )
    #
    #     [wait_for_charge, wait_for_satisfaction]

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

        @task(task_id="combine_blob_names")
        def combine_blob_names(charge_blob_names: list[str], satisfaction_blob_names: list[str]) -> list[str]:
            logging.info(f"Charge Blob Names: {charge_blob_names}")
            logging.info(f"Satisfaction Blob Names: {satisfaction_blob_names}")
            return charge_blob_names + satisfaction_blob_names

        # Define task dependencies
        charge_blob_names = get_blob_names_for_charge('charge/')
        satisfaction_blob_names = get_blob_names_for_satisfaction('satisfaction/')

        # Combine and return the results
        combined_blob_names = combine_blob_names(charge_blob_names, satisfaction_blob_names)

        return combined_blob_names

        # @task(task_id="save_filepath_by_chunks")
        # def save_filepath_by_chunks():
        #     ti = get_current_context()['ti']
        #     charge_data = ti.xcom_pull(task_ids='phase_2_get_blob_names.get_blob_names_for_charge')
        #     satisfaction_data = ti.xcom_pull(task_ids='phase_2_get_blob_names.get_blob_names_for_satisfaction')
        #     logging.info(f"Charge Data: {charge_data}")
        #     logging.info(f"Satisfaction Data: {satisfaction_data}")
        #     return charge_data + satisfaction_data
        #
        # [get_blob_names_for_charge('charge/'), get_blob_names_for_satisfaction('satisfaction/')] >> save_filepath_by_chunks()

    start = EmptyOperator(
        task_id='start',
    )

    # @task_group(group_id='phase_3_create_table_if_not_exists')
    # def phase_3_create_table_if_not_exists():
    #     create_charge_table = PostgresOperator(
    #         task_id=f"create_charge_table",
    #         postgres_conn_id=POSTGRES_CONN_ID,
    #         sql="sql/in_charge.sql",
    #         params={ 'table_name': 'in_charge' }
    #     )
    #
    #     create_satisfaction_table = PostgresOperator(
    #         task_id=f"create_satisfaction_table",
    #         postgres_conn_id=POSTGRES_CONN_ID,
    #         sql="sql/customer_satisfaction.sql",
    #         params={'table_name': 'customer_satisfaction'}
    #     )
    #
    #     create_model_training_table = PostgresOperator(
    #         task_id=f"create_model_training_table",
    #         postgres_conn_id=POSTGRES_CONN_ID,
    #         sql="sql/model_training.sql",
    #         params={'table_name': 'model_training'}
    #     )
    #
    #     [create_charge_table, create_satisfaction_table, create_model_training_table]

    @task(task_id='save_data_from_storage_to_db')
    def save_data_from_storage_to_db(blob_name: str) -> str:
        import requests
        import tempfile
        import os

        logging.warning(f'----------------- BLOB NAME - {blob_name}')
        if blob_name is None:
            return None

        table_name: str = 'in_charge' if 'charge/' in blob_name else 'customer_satisfaction'

        # Download the file from the URL
        url = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{blob_name}'
        response = requests.get(url)

        if response.status_code == 200:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name

            # Now use the local file path for the copy_expert method
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.copy_expert(
                sql=f"COPY {table_name} FROM stdin WITH DELIMITER as ',' CSV HEADER",
                filename=temp_file_path
            )

            # Clean up the temporary file
            os.remove(temp_file_path)
        else:
            raise Exception(f"Failed to download file: {response.status_code} - {response.text}")


    @task(task_id="finish")
    def finish():
        # get from xcom
        logging.warning(f'FINISHHHHHHHHHH ')



    blob_names = phase_2_get_blob_names()
    saved = save_data_from_storage_to_db.partial().expand(blob_name = blob_names)

    start >> blob_names >> saved >> finish()

    # launch_job.partial(batch_job_id=get_job_id()).expand(batch_run_id=get_run_ids())
    # save_data = save_data_from_storage_to_db.partial(table_name='customer_satisfaction').expand(blob_name = get_current_context()['ti'].xcom_pull(task_ids='phase_2_get_blob_names.get_blob_names_for_satisfaction'))


    # start >> phase_1_wait_for_blobs() >> phase_2_get_blob_names() >> phase_3_create_table_if_not_exists()  >> finish()
    # start >> phase_2_get_blob_names() >> save_data() >> finish()



# TRU THIS  https://sihan.hashnode.dev/how-to-use-postreshook-in-airflow
# PostgresHook and copy_expert -  https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py#L117
# sql for hook - https://www.projectpro.io/recipes/extract-data-from-postgres-and-store-it-into-csv-file-by-executing-python-task-airflow-dag
# Apache Airflow & PostgreSQL -  https://medium.com/@sriskandaryan/apache-airflow-postgresql-5f416f841da4
finance_elt()