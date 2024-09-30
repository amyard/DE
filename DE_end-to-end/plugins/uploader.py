import sys
import json
import logging
from abc import ABC, abstractmethod
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable


class Uploader(ABC):
    @abstractmethod
    def upload(self, data):
        pass

    @abstractmethod
    def validation(self):
        pass

    @abstractmethod
    def connection_validation(self):
        pass


class KafkaProducerUploader(Uploader):
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