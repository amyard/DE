import json
import logging
from abc import ABC, abstractmethod
from kafka import KafkaConsumer


class Extract(ABC):
    @abstractmethod
    def extract(self):
        pass



class KafkaConsumerExtract(Extract):
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic

    def extract(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.broker],
            api_version=(7, 6, 1),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000
        )

        messages = []
        try:
            for message in consumer:
                logging.info(f"Received message: {message.value}")
                messages.append(message.value)
        except StopIteration:
            logging.info("No more messages or timeout reached.")

        consumer.close()
        return messages