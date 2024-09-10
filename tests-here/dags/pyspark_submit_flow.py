from pendulum import datetime
import logging

import airflow
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id='pyspark_submit_flow',
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 10)
)
def pyspark_submit_flow():

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark_conn",
        application="jobs/words.py"
    )

    start >> python_job >> finish

pyspark_submit_flow()