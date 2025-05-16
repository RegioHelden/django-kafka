import re
from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.dead_letter.header import DeadLetterHeader
from django_kafka.retry.topic import RetryTopicProducer
from django_kafka.serialization import NoOpSerializer
from django_kafka.topic import TopicProducer

if TYPE_CHECKING:
    from confluent_kafka import cimpl


class DeadLetterTopicProducer(TopicProducer):
    key_serializer = NoOpSerializer
    value_serializer = NoOpSerializer

    def __init__(self, group_id: str, msg: "cimpl.Message"):
        self.group_id = group_id
        self.msg = msg
        super().__init__()

    @classmethod
    def suffix(cls):
        return settings.DEAD_LETTER_TOPIC_SUFFIX

    @property
    def name(self) -> str:
        topic = self.msg.topic()

        if re.search(RetryTopicProducer.pattern(), topic):
            return re.sub(RetryTopicProducer.pattern(), self.suffix(), topic)
        return f"{self.group_id}.{topic}.{self.suffix()}"

    def produce_for(self, header_summary, header_detail):
        self.produce(
            key=self.msg.key(),
            value=self.msg.value(),
            headers=[
                (DeadLetterHeader.MESSAGE_TOPIC, self.msg.topic()),
                (DeadLetterHeader.MESSAGE_PARTITION, str(self.msg.partition())),
                (DeadLetterHeader.MESSAGE_OFFSET, str(self.msg.offset())),
                (DeadLetterHeader.SUMMARY, header_summary),
                (DeadLetterHeader.DETAIL, header_detail),
            ],
        )
