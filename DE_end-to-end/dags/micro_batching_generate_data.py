import os
import json

import pytz
from datetime import timedelta, datetime, time
from dotenv import load_dotenv
from pathlib import Path

from kafka import KafkaProducer
from helpers.micro_batching.generate_mock_data import create_mock_data

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.db import provide_session

dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)


KAFKA_AMOUNT_OF_DATA_IN_BATCH = 50
KAFKA_BOOTSTRAP_SERVER: str = os.environ.get("MICRO_BATCHING_KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC: str = os.environ.get("MICRO_BATCHING_KAFKA_TOPIC")


def convert_to_tz(time_z):
    desired_timezone = pytz.timezone("UTC")
    utc_datetime = datetime.combine(datetime.now(tz=pytz.timezone("UTC")).date(), time_z)
    localized_datetime = pytz.utc.localize(utc_datetime)
    converted_datetime = localized_datetime.astimezone(desired_timezone)

    return converted_datetime

@provide_session
def check_if_task_already_run(dag_id, task_id, session=None) -> bool:
    # FIX ERROR: sqlalchemy.exc.StatementError: (builtins.ValueError) naive datetime is disallowed
    start_of_today = convert_to_tz(time.min)
    end_of_today = convert_to_tz(time.max)

    task_instance = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.task_id == task_id,
        TaskInstance.start_date >= start_of_today,
        TaskInstance.end_date <= end_of_today,
        TaskInstance.state == State.SUCCESS
    ).first()

    return task_instance is not None


default_args = {
    'owner': 'delme',
    'execution_timeout':timedelta(seconds=65),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

@dag(
    start_date=datetime(2024, 9, 14),
    catchup=False,
    schedule_interval=timedelta(minutes=1),
    # schedule_interval=timedelta(seconds=10),
    tags=['micro-batching', 'generate-data', 'delme']
)
def micro_batching_generate_data():
    @task
    def produce_messages():
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            max_block_ms=6 * 10 * 1000,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        for i in range(KAFKA_AMOUNT_OF_DATA_IN_BATCH):
            message = create_mock_data()
            producer.send(KAFKA_TOPIC, message)

        producer.flush()
        producer.close()

    @task.branch(task_id='check_and_trigger_dag')
    def check_and_trigger_dag():
        task_is_run: bool = check_if_task_already_run('micro_batching_generate_data', 'trigger_load_data')
        return ['skip' if task_is_run else 'trigger_load_data']

    trigger_load_data = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id="micro_batching_load_data",
        skip_when_already_exists=True
    )

    start = EmptyOperator(task_id='start')
    skip = EmptyOperator(task_id='skip')

    decision = check_and_trigger_dag()
    start >> produce_messages() >> decision >> [skip, trigger_load_data]

micro_batching_generate_data()