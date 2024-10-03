from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.context import Context
from loader import PostgresLoader
from concurrent.futures import ThreadPoolExecutor
from constants import USERS_INSERT_QUERY, LOGS_INSERT_QUERY, ORDERS_INSERT_QUERY

from faker_orders_generator import FakerGenerator


class CustomPostgresOperator(PostgresOperator):
    def __init__(self, sql: str, postgres_conn_id: str, pg_database: str, *args, **kwargs):
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        super().__init__(sql=self.sql, postgres_conn_id=self.postgres_conn_id, *args, **kwargs)
        self.pg_database = pg_database

    def upload_data(self, query, data):
        PostgresLoader(self.postgres_conn_id, self.pg_database, query, data).upload()

    def execute(self, context: Context) -> None:
        if len(self.sql) > 0:
            super().execute(context)
            return

        fake_generator = FakerGenerator(10, 10, 10)
        fake_generator.generate()

        # psycopg2.errors.ForeignKeyViolation: insert or update on table "logs" violates foreign key constraint "fk_user_email"
        # DETAIL:  Key (user_email)=(ryanhoward@example.org) is not present in table "users".
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_users = executor.submit(self.upload_data, USERS_INSERT_QUERY, fake_generator.users_data)
            future_users.result()

            future_logs = executor.submit(self.upload_data, LOGS_INSERT_QUERY, fake_generator.logs_data)
            future_logs.result()

            future_orders = executor.submit(self.upload_data, ORDERS_INSERT_QUERY, fake_generator.orders_data)
            future_orders.result()