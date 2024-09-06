import re

from django_kafka.dead_letter.headers import DeadLetterHeader
from django_kafka.serialization import NoOpSerializer
from django_kafka.topic import Topic


class DeadLetterTopic(Topic):
    key_serializer = NoOpSerializer
    value_serializer = NoOpSerializer

    key_deserializer = NoOpSerializer
    value_deserializer = NoOpSerializer

    def __init__(self, group_id: str, main_topic: Topic):
        self.group_id = group_id
        self.main_topic = main_topic

    @property
    def name(self) -> str:
        group_id = re.escape(self.group_id)
        if self.main_topic.is_regex():
            topic_name = f"({self.main_topic.name.lstrip('^').rstrip('$')})"
        else:
            topic_name = re.escape(self.main_topic.name)
        return rf"^{group_id}\.{topic_name}\.dlt$"

    def get_produce_name(self, topic_name: str) -> str:
        retry_pattern = r"\.retry\.[0-9]+$"
        if re.search(retry_pattern, topic_name):
            return re.sub(retry_pattern, ".dlt", topic_name)
        return f"{self.group_id}.{topic_name}.dlt"

    def produce_for(self, msg, header_message, header_detail):
        headers = [
            (DeadLetterHeader.MESSAGE, header_message),
            (DeadLetterHeader.DETAIL, header_detail),
        ]
        self.produce(
            name=self.get_produce_name(msg.topic()),
            key=msg.key(),
            value=msg.value(),
            headers=headers,
        )
