import re
from typing import TYPE_CHECKING

from django_kafka.dead_letter.headers import DeadLetterHeader
from django_kafka.retry.topic import RETRY_TOPIC_PATTERN
from django_kafka.serialization import NoOpSerializer
from django_kafka.topic import TopicProducer

if TYPE_CHECKING:
    from confluent_kafka import cimpl

DEAD_LETTER_TOPIC_SUFFIX = "dlt"


class DeadLetterTopicProducer(TopicProducer):
    key_serializer = NoOpSerializer
    value_serializer = NoOpSerializer

    def __init__(self, group_id: str, msg: "cimpl.Message"):
        self.group_id = group_id
        self.msg = msg

    @property
    def name(self) -> str:
        topic = self.msg.topic()
        suffix = DEAD_LETTER_TOPIC_SUFFIX

        if re.search(RETRY_TOPIC_PATTERN, topic):
            return re.sub(RETRY_TOPIC_PATTERN, suffix, topic)
        return f"{self.group_id}.{topic}.{suffix}"

    def produce_for(self, header_message, header_detail):
        headers = [
            (DeadLetterHeader.MESSAGE, header_message),
            (DeadLetterHeader.DETAIL, header_detail),
        ]
        self.produce(
            key=self.msg.key(),
            value=self.msg.value(),
            headers=headers,
        )
