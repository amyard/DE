import logging
import pendulum

from airflow.decorators import dag, task, task_group

logger = logging.getLogger("demo-3-new-flow-logger")


@task()
def python_print_task_1():
    logger.info(f"Inside the python print task.")


@task()
def python_print_task_2():
    logger.info(f"Inside the python print task.")


@task()
def python_print_task_3():
    logger.info(f"Inside the python print task.")


@task()
def python_print_task_4():
    logger.info(f"Inside the python print task.")


@task
def start():
    pass


@task
def end():
    pass


@dag(
    dag_id="demo-3-new-flow",
    start_date=pendulum.datetime(2024, 6, 22, tz="UTC"),
    schedule="0 0 * * *",
    tags=["big-data-course-demo", "new-flow", "illia-example-3"]
)
def pipeline():
    @task_group(group_id="phase_1")
    def phase_1():
        python_print_task_1() >> python_print_task_2()

    @task_group(group_id="phase_2")
    def phase_2():
        python_print_task_3() >> python_print_task_4()

    start() >> phase_1() >> phase_2() >> end()


pipeline()
