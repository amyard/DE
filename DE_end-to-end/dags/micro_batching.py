import logging
import os

from pendulum import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)


AZURE_CONTAINER_NAME: str = 'micro-batching'
AZURE_BLOB_STORAGE_CONN: str ='delme-storage-account'
POSTGRES_CONN_ID: str = "delme-postgresql"

KAFKA_DATA_PRODUCE = 50
KAFKA_BOOTSTRAP_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC_MICRO_BATCHING')


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
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._models import PublicAccess
    from airflow.hooks.base import BaseHook


    @task(task_id='create_storage_container')
    def create_storage_container() -> None:
        def get_conn_string_from_conn_id(conn_id: str) -> str:
            connection = BaseHook.get_connection(conn_id)
            extra = connection.extra_dejson
            connection_string = extra.get('connection_string', '')
            return connection_string

        def get_connection(conn_id: str) -> Optional[BlobServiceClient]:
            try:
                blob_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(conn_id)
                logging.info("Azure Container Client exists")
                return blob_client
            except Exception as ex:
                logging.error(f"Connection failed: {str(ex)}")
                return None

        def create_container(blob_client: Optional[BlobServiceClient], container_name: str) -> bool:
            """
            Create container 'container_name' if not exists
            """
            try:
                containers = blob_client.list_containers()
                if container_name not in containers:
                    blob_client.create_container(container_name, public_access=PublicAccess.CONTAINER)
                    logging.info(f"Container '{container_name}' created'.")
                return True
            except Exception as ex:
                logging.error(f"Error with creating container '{container_name}': {str(ex)}")
                return False

        conn_string = get_conn_string_from_conn_id(AZURE_BLOB_STORAGE_CONN)
        blob_service_client: Optional[BlobServiceClient] = get_connection(conn_string)
        [create_container(blob_service_client, AZURE_CONTAINER_NAME), create_container(blob_service_client, 'checkpoint')]

        blob_service_client.close()


    start = EmptyOperator(task_id='start')

    trigger_generate_data = TriggerDagRunOperator(
        task_id="trigger_generate_data",
        trigger_dag_id="micro_batching_generate_data",
        conf={"message": "Message to pass to Generate Data Dag."},
    )

    # start >> trigger_generate_data
    start >> create_storage_container() >> trigger_generate_data

micro_batching_prepare_env()


@dag(
    start_date=datetime(2024, 9, 14),
    # schedule='@daily',
    catchup=False,
    schedule_interval=timedelta(minutes=3),
    tags=['micro-batching', 'generate-data', 'delme']
)
def micro_batching_generate_data():
    import json
    from kafka import KafkaProducer
    from helpers.micro_batching.generate_mock_data import create_mock_data

    @task
    def produce_messages():
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            max_block_ms=6 * 10 * 1000,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        for i in range(KAFKA_DATA_PRODUCE):
            message = create_mock_data()
            producer.send(KAFKA_TOPIC, message)

        producer.flush()
        producer.close()

    trigger_load_data = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id="micro_batching_load_data",
        skip_when_already_exists=True
    )

    start = EmptyOperator(task_id='start')
    start >> produce_messages() >> trigger_load_data

micro_batching_generate_data()




@dag(
    start_date=datetime(2024, 9, 14),
    schedule='@daily',
    catchup=False,
    # schedule_interval=timedelta(minutes=1),
    tags=['micro-batching', 'generate-load', 'delme']
)
def micro_batching_load_data():
    pass

micro_batching_load_data()


@dag(
    start_date=datetime(2024, 9, 13),
    schedule='@daily',
    catchup=False,
    tags=['micro-batching', 'pyspark', 'delme']
)
def get_data_dag():
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="delme-pyspark",
        application="jobs/micro_batching_load_job.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.27.1"
    )
    start >> python_job >> finish

get_data_dag()