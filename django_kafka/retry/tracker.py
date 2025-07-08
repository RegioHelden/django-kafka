from collections.abc import Iterable
from typing import TYPE_CHECKING, ClassVar

from django_kafka import kafka
from django_kafka.consumer import Consumer, Topics
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import TopicConsumer

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.topic import Topic


class KeyOffsetTrackerTopic(TopicConsumer):
    name = None

    def __init__(self, topic_name: str):
        self.name = topic_name
        super().__init__()

    def consume(self, msg: "cimpl.Message"):
        from django_kafka.models import KeyOffsetTracker

        KeyOffsetTracker.objects.log_msg_offset(msg)
        return True


class KeyOffsetTrackerConsumer(Consumer):
    topics = None
    config: ClassVar = {
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
    }

    def start(self):
        self.run_sanity_checks()
        super().start()

    @classmethod
    def activate(cls, group_id: str) -> None:
        cls.setup(group_id)
        cls.run_sanity_checks()
        kafka.consumers()(cls)

    @classmethod
    def setup(cls, group_id: str):
        cls.config = {**cls.config, "group.id": group_id}
        cls.topics = Topics(*cls.tracked_topics())

    @classmethod
    def run_sanity_checks(cls):
        if not cls.build_config().get("group.id"):
            raise DjangoKafkaError("'group.id' must be specified.")

        if not cls.topics:
            raise DjangoKafkaError(
                "Key offset tracker consumer is registered but there are no topics "
                "configured to use it.",
            )

    @classmethod
    def tracked_topics(cls) -> Iterable["Topic"]:
        topics = []
        for key in kafka.consumers:
            for topic in kafka.consumers[key].topics:
                if topic.retry_settings and topic.retry_settings.use_offset_tracker:
                    topics.append(topic.name)
        return map(KeyOffsetTrackerTopic, set(topics))
