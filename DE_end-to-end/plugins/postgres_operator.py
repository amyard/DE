import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.context import Context
from uploader import PostgresUploader
from concurrent.futures import ThreadPoolExecutor

from faker_orders_generator import FakerGenerator

class CustomPostgresOperator(PostgresOperator):
    def __init__(self, sql: str, postgres_conn_id: str, pg_database: str, *args, **kwargs):
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        super().__init__(sql=self.sql, postgres_conn_id=self.postgres_conn_id, *args, **kwargs)
        self.pg_database = pg_database

    def execute(self, context: Context) -> None:
        if len(self.sql) > 0:
            super().execute(context)
            return

        fake_generator = FakerGenerator(10, 10, 10)
        fake_generator.generate()

        users_insert_query = """
                        INSERT INTO users (first_name, last_name, email, gender, dob, phone_number, address)
                        VALUES %s
                        ON CONFLICT (email) DO NOTHING;
                    """

        logs_insert_query = """
                        INSERT INTO logs (login_date, logout_date, user_email, device)
                        VALUES %s;
                    """

        orders_insert_query = """
                        INSERT INTO orders (customer_email, order_date, delivery_address, product_name, product_category,  product_brand,  product_description,  quantity,  unit_price,  total_price)
                        VALUES %s;
                    """

        def upload_data(query, data):
            PostgresUploader(self.postgres_conn_id, self.pg_database, query, data).upload()

        # psycopg2.errors.ForeignKeyViolation: insert or update on table "logs" violates foreign key constraint "fk_user_email"
        # DETAIL:  Key (user_email)=(ryanhoward@example.org) is not present in table "users".
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_users = executor.submit(upload_data, users_insert_query, fake_generator.users_data)
            future_users.result()

            future_logs = executor.submit(upload_data, logs_insert_query, fake_generator.logs_data)
            future_logs.result()

            future_orders = executor.submit(upload_data, orders_insert_query, fake_generator.orders_data)
            future_orders.result()