import pendulum
from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from kafka_operator import KafkaProducerOperator
from postgres_operator import CustomPostgresOperator

default_args = {
    'owner': 'delme',
    'depends_on_past': False,
    'backfill': False
}


@dag(
    dag_id='test_dags',
    start_date=pendulum.datetime(2024, 9, 29),
    default_args=default_args,
    # schedule_interval=timedelta(days=1),
    schedule='@daily',
    catchup=False,
    description="ETL for dataset",
    tags=['delme', 'dataset-etl']
)
def dataset_etl_dag():
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    @task_group(group_id="generate_tables")
    def generate_tables():
        create_users_table = PostgresOperator(
            task_id='create_users_table',
            postgres_conn_id="delme-postgresql",
            sql="""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    gender VARCHAR(50) NOT NULL,
                    dob TIMESTAMP NOT NULL,
                    phone_number VARCHAR(50),
                    address TEXT NOT NULL
                );
                
                CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            """
        )

        create_logs_table = PostgresOperator(
            task_id='create_logs_table',
            postgres_conn_id="delme-postgresql",
            sql="""
                    CREATE TABLE IF NOT EXISTS logs (
                        id SERIAL PRIMARY KEY,
                        login_date TIMESTAMP NOT NULL,
                        logout_date TIMESTAMP NOT NULL,
                        user_email VARCHAR(255),
                        device VARCHAR(50) NOT NULL,
                        CONSTRAINT fk_user_email FOREIGN KEY (user_email) REFERENCES users(email) ON DELETE SET NULL
                    );
    
                    CREATE INDEX IF NOT EXISTS idx_logs_user_email ON logs(user_email);
                """
        )

        create_orders_table = PostgresOperator(
            task_id='create_orders_table',
            postgres_conn_id="delme-postgresql",
            sql="""
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        customer_email VARCHAR(255),
                        order_date TIMESTAMP NOT NULL,
                        delivery_address TEXT NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        product_category VARCHAR(100),
                        product_brand VARCHAR(100),
                        product_description TEXT,
                        quantity INT NOT NULL CHECK (quantity > 0),
                        unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
                        total_price DECIMAL(10, 2) NOT NULL CHECK (total_price >= 0),
                        CONSTRAINT fk_customer_email FOREIGN KEY (customer_email) REFERENCES users(email) ON DELETE SET NULL
                    );
    
                    CREATE INDEX IF NOT EXISTS idx_logs_user_email ON logs(user_email);
                """
        )

        create_users_table >> create_logs_table >> create_orders_table

    generate_data = KafkaProducerOperator(
        task_id='generate_data',
        broker = 'broker:29092'
    )

    generate_data_for_postgres = CustomPostgresOperator(
        task_id='generate_data_for_postgres',
        postgres_conn_id="delme-postgresql",
        pg_database="postgres",
        sql=""
    )

    # start >> generate_data >> finish
    # start >> generate_tables() >> finish
    start >> generate_data_for_postgres >> finish


dataset_etl_dag()