import pendulum

from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


from kafka_operator import KafkaProducerOperator
from postgres_operator import CustomPostgresOperator
from faker_orders_generator import FakerGenerator
from extract import KafkaConsumerExtract
from loader import PostgresLoader
from constants import USERS_INSERT_QUERY, LOGS_INSERT_QUERY, ORDERS_INSERT_QUERY


default_args = {
    'owner': 'delme',
    'depends_on_past': False,
    'backfill': False
}

def get_postgres_credentials_from_conn_id(conn_id: str) -> tuple:
    connection = BaseHook.get_connection(conn_id)
    login = connection.login
    password = connection.password
    host = connection.host
    db_name = connection.schema
    port = f'{connection.port}'

    login = 'admin' if '*' in login else login
    password = 'admin' if '*' in password else password

    return (login, password, host, port, db_name)


POSTGRES_CONN_ID: str = 'delme-postgresql-clientdb'
LOGS_TOPIC: str = 'logs_topic'
USERS_TOPIC: str = 'users_topic'
ORDERS_TOPIC: str = 'orders_topic'
KAFKA_BROKER: str = 'broker:29092'
(POSTGRES_LOGIN, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME) = get_postgres_credentials_from_conn_id(POSTGRES_CONN_ID)


@dag(
    dag_id='superset_etl',
    start_date=pendulum.datetime(2024, 9, 29),
    default_args=default_args,
    # schedule_interval=timedelta(days=1),
    schedule='@daily',
    catchup=False,
    description="ETL for superset",
    tags=['delme', 'superset-etl']
)
def superset_etl_dag():
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    @task_group(group_id="generate_tables")
    def generate_tables():
        create_users_table = PostgresOperator(
            task_id='create_users_table',
            postgres_conn_id=POSTGRES_CONN_ID,
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
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="""
                    CREATE TABLE IF NOT EXISTS logs (
                        id SERIAL PRIMARY KEY,
                        login_date TIMESTAMP NOT NULL,
                        logout_date TIMESTAMP NOT NULL,
                        user_email VARCHAR(255),
                        device VARCHAR(50) NOT NULL
                    );
    
                    CREATE INDEX IF NOT EXISTS idx_logs_user_email ON logs(user_email);
                """
        )

        create_orders_table = PostgresOperator(
            task_id='create_orders_table',
            postgres_conn_id=POSTGRES_CONN_ID,
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
                        total_price DECIMAL(10, 2) NOT NULL CHECK (total_price >= 0)
                    );
    
                    CREATE INDEX IF NOT EXISTS idx_logs_user_email ON logs(user_email);
                """
        )

        create_users_table >> [create_logs_table, create_orders_table]

    @task_group(group_id="generate_data")
    def generate_data():
        fake_generator = FakerGenerator(150, 1200, 2500)
        # fake_generator = FakerGenerator(10, 10, 10)
        fake_generator.generate()

        generate_data_for_kafka = KafkaProducerOperator.partial(
            task_id='generate_data_for_kafka',
            broker = KAFKA_BROKER
        ).expand_kwargs([
            {'topic': LOGS_TOPIC, 'data': fake_generator.logs_data, 'column_names': fake_generator.logs_columns},
            {'topic': USERS_TOPIC, 'data': fake_generator.users_data, 'column_names': fake_generator.users_columns},
            {'topic': ORDERS_TOPIC, 'data': fake_generator.orders_data, 'column_names': fake_generator.orders_columns},
        ])

        generate_data_for_postgres = CustomPostgresOperator(
            task_id='generate_data_for_postgres',
            postgres_conn_id=POSTGRES_CONN_ID,
            pg_database="clientdb",
            sql=""
        )

        [generate_data_for_kafka, generate_data_for_postgres]

    @task_group(group_id="retrieve_data")
    def retrieve_data():
        @task(task_id='get_data_from_topic')
        def get_data_from_topic(topic):
            messages: list[list] = KafkaConsumerExtract(broker=KAFKA_BROKER, topic=topic).extract()
            cleaned_messages: list[list] = [list(d.values()) for d in messages]
            return cleaned_messages

        @task(task_id='store_data_from_topic')
        def store_data_from_topic(query: str, data: list[list]):
            if len(data) > 0:
                PostgresLoader(POSTGRES_CONN_ID, "clientdb", query, data).upload()

        users_kafka = store_data_from_topic.override(task_id=f'store_data_from_{USERS_TOPIC}')(
            USERS_INSERT_QUERY,
            get_data_from_topic.override(task_id=f'get_data_from_{USERS_TOPIC}')(USERS_TOPIC)
        )

        logs_kafka = store_data_from_topic.override(task_id=f'store_data_from_{LOGS_TOPIC}')(
            LOGS_INSERT_QUERY,
            get_data_from_topic.override(task_id=f'get_data_from_{LOGS_TOPIC}')(LOGS_TOPIC)
        )

        orders_kafka = store_data_from_topic.override(task_id=f'store_data_from_{ORDERS_TOPIC}')(
            ORDERS_INSERT_QUERY,
            get_data_from_topic.override(task_id=f'get_data_from_{ORDERS_TOPIC}')(ORDERS_TOPIC)
        )

        users_kafka >> logs_kafka
        logs_kafka >> orders_kafka

    # Define application arguments
    application_args = [
        # Section Postgres
        "--POSTGRES_LOGIN", POSTGRES_LOGIN,
        "--POSTGRES_PASSWORD", POSTGRES_PASSWORD,
        "--POSTGRES_HOST", POSTGRES_HOST,
        "--POSTGRES_PORT", POSTGRES_PORT,
        "--POSTGRES_DBNAME", POSTGRES_DBNAME,
    ]

    cleaning_data_silver_layer = SparkSubmitOperator(
        task_id="cleaning_data_silver_layer",
        conn_id="delme-pyspark",
        application="jobs/superset_cleaning.py",
        packages="org.postgresql:postgresql:42.7.3",
        application_args=application_args
    )

    transform_data_gold_layer = SparkSubmitOperator(
        task_id="transform_data_gold_layer",
        conn_id="delme-pyspark",
        application="jobs/superset_transform.py",
        packages="org.postgresql:postgresql:42.7.3",
        application_args=application_args
    )

    start >> generate_tables() >> generate_data() >> retrieve_data() >> cleaning_data_silver_layer >> transform_data_gold_layer >> finish


superset_etl_dag()
