from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

from faker_orders_generator import FakerGenerator
from uploader import KafkaProducerUploader


class KafkaProducerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, broker, *args, **kwargs):
        super(KafkaProducerOperator, self).__init__(*args, **kwargs)
        self.broker = broker

    def execute(self, context):
        # generate fake data
        # fake_generator = FakerGenerator(150, 1200, 2500)
        fake_generator = FakerGenerator(10, 10, 10)
        fake_generator.generate()

        # upload fake data into kafka
        KafkaProducerUploader(broker=self.broker, topic='logs_topic', data=fake_generator.logs_data, column_names=fake_generator.logs_columns).upload()
        KafkaProducerUploader(broker=self.broker, topic='users_topic', data=fake_generator.users_data, column_names=fake_generator.users_columns).upload()
        KafkaProducerUploader(broker=self.broker, topic='orders_topic', data=fake_generator.orders_data, column_names=fake_generator.orders_columns).upload()


