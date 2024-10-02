import sys
import json
import logging
from abc import ABC, abstractmethod

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Loader(ABC):
    @abstractmethod
    def upload(self, data):
        pass

    @abstractmethod
    def validation(self):
        pass

    @abstractmethod
    def connection_validation(self):
        pass


class KafkaProducerLoader(Loader):
    def __init__(self, broker, topic, data, column_names):
        self.broker: str = broker
        self.topic: str = topic
        self.data: list[list] = data
        self.column_names: list = column_names

    def upload(self):
        if not self.validation() and not self.connection_validation():
            return sys.exit(1)

        producer = KafkaProducer(
            bootstrap_servers=self.broker,
            max_block_ms = 6 * 10 * 1000,
            acks=1,
            batch_size=32 * 1024,
            linger_ms=5,
            # compression_type='lz4',
            retries=5,
            retry_backoff_ms=300,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for item in self.data:
            producer.send(self.topic, dict(zip(self.column_names, item)))
            logging.info(f"Data was send to topic {self.topic}: {item}")

        producer.flush()
        producer.close()
        logging.info(f"Data was successfully uploaded")

    def validation(self) -> bool:
        topics: dict[str, str] = {
            "broker": self.broker,
            "topic": self.topic
        }

        for key, value in topics.items():
            if not value:
                logging.warning(f"{key.replace('_', ' ').title()} not set")
                return False

        if len(self.data) == 0:
            logging.warning(f"No data was provided for {topics['topic']}")
            return False

        return True

    def connection_validation(self) -> bool:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.broker,
            )
            return consumer.bootstrap_connected()
        except NoBrokersAvailable as ex:
            logging.error(f"{ex}")
            return False


class PostgresLoader(Loader):
    def __init__(self, postgres_conn_id: str, pg_database: str, query: str, data: list[list]):
        self.postgres_conn_id: str = postgres_conn_id
        self.pg_database: str = pg_database
        self.query: str = query
        self.data: list[list] = data
        self.hook = PostgresHook(postgres_conn_id = self.postgres_conn_id, schema = self.pg_database)
        self.connection = self.hook.get_conn()

    def upload(self):
        if not self.validation() and not self.connection_validation():
            return sys.exit(1)

        try:
            cursor = self.connection.cursor()
            execute_values(cursor, self.query, self.data)
            logging.info(f"Batch data inserted into 'users' table.")
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Failed to insert batch data: {e}")
            raise
        finally:
            cursor.close()
            self.connection.close()

    def validation(self):
        return not self.postgres_conn_id and not self.pg_database and not self.sql and len(self.data) > 0

    def connection_validation(self) -> bool:
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1;")
            cursor.fetchall()
            cursor.close()
            return True
        except Exception as ex:
            return False
