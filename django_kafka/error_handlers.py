from confluent_kafka import KafkaError

from django_kafka.exceptions import DjangoKafkaError


class ClientErrorHandler:
    def __call__(self, error: KafkaError):
        if error.fatal():
            raise DjangoKafkaError(error)
