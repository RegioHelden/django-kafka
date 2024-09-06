import re

from confluent_kafka import cimpl

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.retry.headers import RetryHeader
from django_kafka.serialization import NoOpSerializer
from django_kafka.topic import Topic


class RetryTopic(Topic):
    key_serializer = NoOpSerializer
    value_serializer = NoOpSerializer

    key_deserializer = NoOpSerializer
    value_deserializer = NoOpSerializer

    def __init__(self, group_id: str, main_topic: Topic):
        if not main_topic.retry_settings:
            raise DjangoKafkaError(f"Topic {main_topic} is not marked for retry")
        self.main_topic = main_topic
        self.group_id = group_id

    @property
    def name(self) -> str:
        group_id = re.escape(self.group_id)
        if self.main_topic.is_regex():
            topic_name = f"({self.main_topic.name.lstrip('^').rstrip('$')})"
        else:
            topic_name = re.escape(self.main_topic.name)

        return rf"^{group_id}\.{topic_name}\.retry\.[0-9]+$"

    @classmethod
    def get_attempt(cls, topic_name: str) -> int:
        match = re.search(r"\.retry\.([0-9]+)$", topic_name)
        return int(match.group(1)) if match else 0

    def get_produce_name(self, topic_name: str, attempt: int) -> str:
        if attempt == 1:
            return f"{self.group_id}.{topic_name}.retry.1"
        return re.sub(r"\.[0-9]+$", f".{attempt}", topic_name)

    def consume(self, msg: cimpl.Message):
        self.main_topic.consume(msg)

    def retry_for(self, msg: cimpl.Message, exc: Exception) -> bool:
        """
        msg: the message that failed, this may come from the main topic or retry topic
        exc: Exception that caused the failure
        """
        settings = self.main_topic.retry_settings

        if not settings.should_retry(exc=exc):
            return False

        topic_name = msg.topic()
        attempt = self.get_attempt(topic_name) + 1

        if settings.attempts_exceeded(attempt):
            return False

        self.produce(
            name=self.get_produce_name(topic_name, attempt),
            key=msg.key(),
            value=msg.value(),
            headers=[
                (RetryHeader.MESSAGE, str(exc)),
                (RetryHeader.TIMESTAMP, settings.get_retry_timestamp(attempt)),
            ],
        )
        return True
