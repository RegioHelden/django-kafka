import re
from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.retry.header import RetryHeader
from django_kafka.serialization import NoOpSerializer
from django_kafka.topic import TopicConsumer, TopicProducer

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.retry.settings import RetrySettings


class RetryTopicProducer(TopicProducer):
    key_serializer = NoOpSerializer
    value_serializer = NoOpSerializer

    def __init__(
        self,
        group_id: str,
        retry_settings: "RetrySettings",
        msg: "cimpl.Message",
    ):
        self.settings = retry_settings
        self.group_id = group_id
        self.msg = msg
        self.attempt = self.get_next_attempt(msg.topic())
        super().__init__()

    @classmethod
    def suffix(cls):
        return settings.RETRY_TOPIC_SUFFIX

    @classmethod
    def pattern(cls):
        """returns regex pattern to match topics producing by this class"""
        return rf"{re.escape(cls.suffix())}\.([0-9]+)$"

    @classmethod
    def get_next_attempt(cls, topic_name: str) -> int:
        match = re.search(cls.pattern(), topic_name)
        attempt = int(match.group(1)) if match else 0
        return attempt + 1

    @property
    def name(self) -> str:
        topic = self.msg.topic()
        suffix = f"{self.suffix()}.{self.attempt}"

        if re.search(self.pattern(), topic):
            return re.sub(self.pattern(), suffix, topic)
        return f"{self.group_id}.{self.msg.topic()}.{suffix}"

    def retry(self, exc: Exception) -> bool:
        if not self.settings.should_retry(exc=exc):
            return False

        if self.settings.attempts_exceeded(attempt=self.attempt):
            return False

        self.produce(
            key=self.msg.key(),
            value=self.msg.value(),
            headers=[
                (RetryHeader.MESSAGE, str(exc)),
                (
                    RetryHeader.TIMESTAMP,
                    self.settings.get_retry_timestamp(self.attempt),
                ),
            ],
        )
        return True


class RetryTopicConsumer(TopicConsumer):
    key_deserializer = NoOpSerializer
    value_deserializer = NoOpSerializer

    def __init__(self, group_id: str, topic_consumer: TopicConsumer):
        if not topic_consumer.retry_settings:
            raise DjangoKafkaError(
                f"TopicConsumer {topic_consumer} is not marked for retry",
            )
        self.topic_consumer = topic_consumer
        self.group_id = group_id
        super().__init__()

    @property
    def name(self) -> str:
        """returns name as regex pattern matching all attempts on the group's topic"""
        group_id = re.escape(self.group_id)
        if self.topic_consumer.is_regex():
            topic_name = f"({self.topic_consumer.name.lstrip('^').rstrip('$')})"
        else:
            topic_name = re.escape(self.topic_consumer.name)

        return rf"^{group_id}\.{topic_name}\.{RetryTopicProducer.pattern()}"

    def consume(self, msg: "cimpl.Message"):
        self.topic_consumer.consume(msg)

    def producer_for(self, msg: "cimpl.Message") -> RetryTopicProducer:
        return RetryTopicProducer(
            group_id=self.group_id,
            retry_settings=self.topic_consumer.retry_settings,
            msg=msg,
        )
