import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer


class KafkaProducerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, broker, topic, num_records, *args, **kwargs):
        super(KafkaProducerOperator, self).__init__(*args, **kwargs)
        self.broker = broker
        self.topic = topic
        self.num_records = num_records

    def generate_data(self, row_numbers):
        return "asd"

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        for row_number in range(self.num_records):
            transaction = self.generate_data(row_number)
            producer.send(self.topic, transaction)
            self.log.info(f"Data was send: {transaction}")

        producer.flush()
        producer.close()
        self.log.info(f"Data {self.num_records} was sent to {self.broker}")


# class KafkaProducerOperatorPlugin(AirflowPlugin):
#     name = "kafka_producer_operator"
#     operators = [KafkaProducerOperator]