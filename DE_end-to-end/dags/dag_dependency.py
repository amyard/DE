import random
from pendulum import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from tornado.process import task_id


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

    @task()
    def random_generator() -> int:
        return random.randint(0, 9)

    @task.branch(task_id='make_decission')
    def make_decision(number: int) -> list[str]:
        redirect: str = 'trigger_A' if number  % 2 == 0 else 'trigger_B'
        return [redirect]


    number = random_generator()
    decision = make_decision(number)
    start >> number >> decision >> [trigger_A, trigger_B] >> finish

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