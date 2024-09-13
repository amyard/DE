from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

@dag(
    start_date=datetime(2024, 9,13),
    schedule='@daily',
    catchup=False,
    dag_id="parent_dag"
)
def parent_dag():
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    trigger_A = TriggerDagRunOperator(
        task_id="trigger_A",
        trigger_dag_id="slave_dag_1",
        conf={"message": "Message to pass to downstream DAG A."},
    )
    trigger_B = TriggerDagRunOperator(
        task_id="trigger_B",
        trigger_dag_id="slave_dag_2",
    )

    start >> [trigger_A, trigger_B] >> finish
parent_dag()


@dag(
    start_date=datetime(2024, 9,13),
    schedule='@daily',
    catchup=False,
    dag_id="slave_dag_1"
)
def slave_dag_1():
    downstream_task = BashOperator(
        task_id="downstream_task",
        bash_command='echo "Upstream message: $message"',
        env={"message": '{{ dag_run.conf.get("message") }}'},
    )

    downstream_task
slave_dag_1()


@dag(
    start_date=datetime(2024, 9,13),
    schedule='@daily',
    catchup=False,
    dag_id="slave_dag_2"
)
def slave_dag_2():
    downstream_task = BashOperator(
        task_id="downstream_task",
        bash_command='echo "Upstream message: $message"',
        env={"message": '{{ dag_run.conf.get("message") }}'},
    )

    next = EmptyOperator(task_id='next')

    downstream_task >> next
slave_dag_2()