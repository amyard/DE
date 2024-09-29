import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from kafka_operator import KafkaProducerOperator
# from airflow.decorators import KafkaProducerOperatorPlugin

default_args = {
    'owner': 'delme',
    'depends_on_past': False,
    'backfill': False
}


@dag(
    dag_id='test_dags',
    start_date=pendulum.datetime(2024, 9, 29),
    default_args=default_args,
    # schedule_interval=timedelta(days=1),
    schedule='@daily',
    catchup=False,
    description="Data generator",
    tags=['delme', 'test-dags']
)
def test_dags():
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    generate_data = KafkaProducerOperator(
        task_id='generate_data',
        broker = 'broker:29092',
        topic = 'test_topic',
        num_records = 50
    )

    start >> generate_data >> finish


test_dags()