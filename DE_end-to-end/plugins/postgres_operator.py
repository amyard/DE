import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.context import Context
from uploader import PostgresUploader

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

        PostgresUploader(self.postgres_conn_id, self.pg_database, users_insert_query, fake_generator.users_data).upload()
        PostgresUploader(self.postgres_conn_id, self.pg_database, logs_insert_query, fake_generator.logs_data).upload()
        PostgresUploader(self.postgres_conn_id, self.pg_database, orders_insert_query, fake_generator.orders_data).upload()