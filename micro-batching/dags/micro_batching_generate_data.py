import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import timedelta, datetime
from helpers.generate_mock_data import create_mock_data

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv()

KAFKA_DATA_PRODUCER = 50
KAFKA_BOOTSTRAP_SERVER = 'broker:29092'
KAFKA_TOPIC = 'micro_batching_topic'

default_kwargs = {
    'owner': 'delme',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='micro_batching_generate_data',
    default_args=default_kwargs,
    schedule_interval=timedelta(minutes=5),
    catchup=False
)
def micro_batching_generate_data():
    @task(task_id='produce_messages')
    def produce_messages():
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            max_block_ms = 6 * 10 * 1000,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        for i in range(KAFKA_DATA_PRODUCER):
            message = create_mock_data()
            producer.send(KAFKA_TOPIC, message)

        producer.flush()
        producer.close()

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    start >> produce_messages() >> finish

micro_batching_generate_data()


@dag(
    dag_id='micro_batching_elt',
    default_args=default_kwargs,
    schedule_interval=timedelta(minutes=5),
    catchup=False
)
def micro_batching_elt():
    from pyspark.sql import SparkSession

    @task(task_id='spark_initialize')
    def spark_initialize():
        spark = SparkSession\
            .builder\
            .appName('Micro Batching ELT') \
            .config("spark.sql.shuffle.partitions", "10")\
            .config("spark.executor.instances", "2")\
            .config("spark.executor.cores", "2")\
            .config("spark.driver.memory", "2g")\
            .config("spark.executor.memory", "2g")\
            .config("spark.network.timeout", "600s")\
            .config("spark.executor.heartbeatInterval", "60s")\
            .config("spark.rpc.message.maxSize", "200") \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2") \
            .getOrCreate()


    @task(task_id='kafka_consumer')
    def kafka_consumer():
        pass

micro_batching_elt()