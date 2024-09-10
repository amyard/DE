import logging
import pendulum
import random

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger("demo-4-new-flow-logger")

@dag(
    dag_id="demo-4-new-flow",
    start_date=pendulum.datetime(2024, 6, 22, tz="UTC"),
    schedule="0 0 * * *",
    tags=["big-data-course-demo", "new-flow", "illia-example-4"]
)
def pipeline():
    @task()
    def random_generator() -> int:
        return random.randint(0, 9)

    @task.branch(task_id="is_odd")
    def is_odd(number: int) -> list[str]:
        if number % 2 == 0:
            return ["phase_1.even_handler"]
        else:
            return ["phase_1.odd_handler"]

    @task(task_id="odd_handler")
    def odd_handler():
        pass

    @task(task_id="even_handler")
    def even_handler():
        pass

    start = EmptyOperator(
        task_id="start"
    )

    @task_group(group_id="phase_1")
    def phase_1():
        number = random_generator()

        is_odd(number) >> [even_handler(), odd_handler()]

    start >> phase_1()


pipeline()
