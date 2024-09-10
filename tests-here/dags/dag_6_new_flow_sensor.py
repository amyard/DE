import logging
import random

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue

logger = logging.getLogger("demo-6-new-flow-logger")


@dag(
    dag_id="demo-6-new-flow",
    start_date=pendulum.datetime(2024, 6, 22, tz="UTC"),
    schedule="0 0 * * *",
    tags=["big-data-course-demo", "new-flow", "illia-example-6"]
)
def pipeline():
    # Mode poke/reschedule
    @task.sensor(poke_interval=5, timeout=100, mode="reschedule")
    def wait_for_upstream() -> PokeReturnValue:
        number = random.randint(0, 4)
        logger.info(f"Current number is {number}")
        if number == 1:
            return PokeReturnValue(is_done=True, xcom_value=number)
        else:
            return PokeReturnValue(is_done=False, xcom_value=number)

    @task
    def finish(number: int):
        logger.info(f"Task done received valid integer {number}")

    finish(wait_for_upstream())


pipeline()
