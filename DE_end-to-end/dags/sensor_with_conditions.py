import logging
import pendulum
from dotenv import load_dotenv
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

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

    def blob_connection(task_id: str, container_name: str, prefix: str) -> WasbPrefixSensor:
        return WasbPrefixSensor(
            task_id=task_id,
            wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
            container_name=container_name,
            poke_interval=POKE_INTERVAL,
            timeout=TIMEOUT_INTERVAL,
            mode='reschedule',
            deferrable=True,
            prefix=prefix,
            on_failure_callback=_failure_callback,
            soft_fail=True
        )

    blob_sensor_1 = blob_connection("blob_sensor_1", AZURE_CONTAINER_NANE, 'sensor')
    blob_sensor_2 = blob_connection("blob_sensor_2", AZURE_CONTAINER_NANE, 'notexistblob')
    blob_sensor_3 = blob_connection("blob_sensor_3", AZURE_CONTAINER_NANE, 'some_blob')

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

    def get_sql_query(table_name: str) -> str:
        # Use double quotes to escape the table name
        return f"SELECT EXISTS (SELECT 1 FROM {table_name});"

    postgres_conn_1 = SqlSensor(
        task_id="postgres_conn_1",
        conn_id=POSTGRES_CONN_ID,
        sql="""SELECT EXISTS (
                    SELECT 1 
                    FROM {{ params.table_name }} 
                    WHERE DATE(created) = '{{ ds }}'
                );""",
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        on_failure_callback=_failure_callback,
        soft_fail=True,
        params={"table_name": "magic"}
    )

    postgres_conn_2 = SqlSensor(
        task_id="postgres_conn_2",
        conn_id=POSTGRES_CONN_ID,
        sql="""SELECT EXISTS (
                    SELECT 1 
                    FROM {{ params.table_name }} 
                    WHERE DATE(created) = '{{ ds }}'
                );""",
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_INTERVAL,
        mode='reschedule',
        on_failure_callback=_failure_callback,
        soft_fail=True,
        params={"table_name": "technology"}
    )

    @task(task_id='all_success', trigger_rule=TriggerRule.ALL_SUCCESS)
    def all_success():
        logging.info("Done method - trigger_rule='none_failed_min_one_success'.")

    @task(trigger_rule='none_failed_min_one_success')
    def done():
        logging.info("Done method - trigger_rule='none_failed_min_one_success'.")

    start >> [blob_sensor_1, blob_sensor_2, blob_sensor_3, postgres_conn_1, postgres_conn_2]
    [blob_sensor_1, blob_sensor_2, blob_sensor_3] >> blob_sensor_4 >> done() >> finish
    [postgres_conn_1, postgres_conn_2] >> all_success()



sensor_with_conditions()

