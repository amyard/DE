from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from loader import KafkaProducerLoader


class KafkaProducerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, broker: str, topic: str, column_names: list, data: list[list], *args, **kwargs):
        super(KafkaProducerOperator, self).__init__(*args, **kwargs)
        self.broker = broker
        self.topic = topic
        self.column_names = column_names
        self.data = data

    def execute(self, context):
        KafkaProducerLoader(self.broker, self.topic, self.data, self.column_names).upload()