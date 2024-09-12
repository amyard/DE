import logging
import pendulum
from dotenv import load_dotenv
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.exceptions import AirflowSensorTimeout


dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)

AZURE_CONTAINER_NANE: str = 'finance-data'
AZURE_BLOB_STORAGE_CONN: str ='delme-storage-account'
POSTGRES_CONN_ID: str = "delme-postgresql"

POKE_INTERVAL = 1 * 5
TIMEOUT_INTERVAL = 1 * 15


def _failure_callback(context):
  if isinstance(context['exception'], AirflowSensorTimeout):
    print(context)
    print("Sensor timed out")


default_args = {
    'owner': 'delme'
}

@dag(
    dag_id='sensor_with_conditions',
    start_date=pendulum.datetime(2024, 9, 4),
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['sensor', 'delme', 'chain']
)
def sensor_with_conditions():

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    blob_sensor_1 = WasbPrefixSensor(
        task_id="blob_sensor_1",
        wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
        container_name='finance-data',
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        deferrable=True,
        prefix='sensor',
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    blob_sensor_2 = WasbPrefixSensor(
        task_id="blob_sensor_2",
        wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
        container_name='finance-data',
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        deferrable=True,
        prefix='notexistblob',
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    blob_sensor_3 = WasbPrefixSensor(
        task_id="blob_sensor_3",
        wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
        container_name='finance-data',
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        deferrable=True,
        prefix='some_blob',
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    blob_sensor_4 = WasbPrefixSensor.partial(
        task_id="blob_sensor_4",
        wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        deferrable=True,
        on_failure_callback=_failure_callback,
        soft_fail=True,
        trigger_rule='none_failed_min_one_success'
    ).expand_kwargs([
        {'prefix': 'sensor', 'container_name': AZURE_CONTAINER_NANE},
        {'prefix': 'not_exist_blob', 'container_name': AZURE_CONTAINER_NANE},
        {'prefix': 'some_blob', 'container_name': AZURE_CONTAINER_NANE},
    ])

    @task(trigger_rule='none_failed_min_one_success')
    def done():
        logging.info("Done method - trigger_rule='none_failed_min_one_success'.")

    start >> [blob_sensor_1, blob_sensor_2, blob_sensor_3] >> blob_sensor_4 >> done() >> finish


sensor_with_conditions()

