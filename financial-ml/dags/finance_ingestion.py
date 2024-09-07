"""
### Helper DAG that generated mock data for the finance_elt DAG

This DAG runs a script located in `helpers/create_mock_data.py` that generates
mock data for the finance_elt DAG and loads that mock data to S3/MinIO.
"""

import os
import logging
from pendulum import datetime
from dotenv import load_dotenv
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from  azure. storage. blob._models import PublicAccess
from airflow.decorators import task, dag
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from helpers.create_mock_data import generate_mock_data
from typing import Optional, List, Dict

dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv()

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LOCAL_FOLDER_PATH: str = "dags/helpers/mock_data"
AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW: str = 'delme-storage-account'
AZURE_CONTAINER_NAME: Optional[str] = os.environ.get('AZURE_CONTAINER_NAME')
AZURE_BLOB_STORAGE_CONN: Optional[str] = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')


@dag(
    start_date=datetime(2024, 9, 3),
    schedule='@daily',
    catchup=False,
    tags=['helper', 'mock_data', 'delme', 'sql']
)
def finance_generate_and_upload():
    @task
    def generate_mock_data_task() -> None:
        logging.info("generate_mock_data_task")
        generate_mock_data(LOCAL_FOLDER_PATH)

    @task
    def prepare_kwargs_for_replacement_task() -> List[Dict[str, str]]:
        logging.info("prepare_kwargs_for_replacement_task")

        list_of_kwargs = []
        for file_name in os.listdir(LOCAL_FOLDER_PATH):
            logging.info(f"file_name: {file_name}")

            stripe_type: str = 'charge' if "charge" in file_name else 'satisfaction'
            kwarg_dict: Dict[str, str] = {
                'file_path': f"{LOCAL_FOLDER_PATH}/{file_name}",
                'blob_name': f"{stripe_type}/{file_name}"
            }
            logging.info(f"kwarg_dict: {kwarg_dict}")
            list_of_kwargs.append(kwarg_dict)

        return list_of_kwargs

    upload_data_kwargs: List[Dict[str, str]] = prepare_kwargs_for_replacement_task()
    logging.info(f"upload_data_kwargs - {upload_data_kwargs}")
    generate_mock_data_task() >> upload_data_kwargs

    @task
    def create_storage_container() -> None:
        def get_connection() -> Optional[BlobServiceClient]:
            try:
                blob_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(AZURE_BLOB_STORAGE_CONN)
                logging.info("Azure Container Client exists")
                return blob_client
            except Exception as ex:
                logging.error(f"Connection failed: {str(ex)}")
                return None

        def create_container(blob_client: Optional[BlobServiceClient], container_name: str) -> bool:
            """
            Create container 'AZURE_CONTAINER_NAME' if not exists
            """
            try:
                containers = blob_client.list_containers()
                if AZURE_CONTAINER_NAME not in containers:
                    blob_client.create_container(container_name, public_access=PublicAccess.CONTAINER)
                    logging.info(f"Container '{AZURE_CONTAINER_NAME}' created'.")
                return True
            except Exception as ex:
                logging.error(f"Error with creating container '{AZURE_CONTAINER_NAME}': {str(ex)}")
                return False

        blob_service_client: Optional[BlobServiceClient] = get_connection()
        [create_container(blob_service_client, AZURE_CONTAINER_NAME), create_container(blob_service_client, 'archive')]

        blob_service_client.close()

    upload_mock_data = LocalFilesystemToWasbOperator.partial(
        wasb_conn_id = AZURE_BLOB_STORAGE_CONN_FROM_AIRFLOW,
        task_id='upload_mock_data',
        container_name=AZURE_CONTAINER_NAME,
        load_options={"overwrite": True}
    ).expand_kwargs(upload_data_kwargs)

    create_storage_container() >> upload_mock_data

finance_generate_and_upload()