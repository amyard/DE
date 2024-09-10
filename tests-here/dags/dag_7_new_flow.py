import logging

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger("demo-7-new-flow-logger")


@dag(
    dag_id="demo-7-new-flow",
    start_date=pendulum.datetime(2024, 6, 22, tz="UTC"),
    schedule="0 0 * * *",
    tags=["big-data-course-demo", "new-flow", "illia-example-7"]
)
def pipeline():
    @task
    def get_job_id() -> int:
        return 1234

    @task
    def get_run_ids() -> list[int]:
        return [23, 21, 32, 467]

    @task()
    def launch_job(batch_job_id: int, batch_run_id: int) -> int:
        logger.info(f"Launched run {batch_run_id} for job {batch_job_id}")
        return batch_run_id

    @task()
    def move_to(batch_run_id: int):
        logger.info(f"MOVE TO TASK run {batch_run_id}")

    # WORK !!
    # launched_run_ids = launch_job.partial(batch_job_id=get_job_id()).expand(batch_run_id=get_run_ids())
    # move_to.expand(batch_run_id=launched_run_ids)

    launched_run_ids = launch_job.partial(batch_job_id=get_job_id()).expand(batch_run_id=get_run_ids())
    moved = move_to.partial().expand(batch_run_id=launched_run_ids)

    moved

    # default behaviour
    # launch_job.partial(batch_job_id=get_job_id()).expand(batch_run_id=get_run_ids())


pipeline()
