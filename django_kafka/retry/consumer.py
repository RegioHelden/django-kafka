import traceback
from typing import TYPE_CHECKING

from django.utils import timezone

from django_kafka.conf import settings
from django_kafka.consumer import Consumer, Topics
from django_kafka.dead_letter.topic import DeadLetterTopicProducer
from django_kafka.retry.header import RetryHeader
from django_kafka.retry.topic import RetryTopicConsumer

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.topic import TopicConsumer


class RetryTopics(Topics):
    def __init__(self, group_id: str, *topic_consumers: "TopicConsumer"):
        super().__init__(
            *(
                RetryTopicConsumer(group_id=group_id, topic_consumer=t)
                for t in topic_consumers
            ),
        )


class RetryConsumer(Consumer):
    topics: RetryTopics

    @classmethod
    def build(cls, consumer_cls: type["Consumer"]) -> type["RetryConsumer"] | None:
        """Generates RetryConsumer subclass linked to consumer class retryable topics"""
        retryable_topics = consumer_cls.topics.get_retryable(blocking=False)
        if not retryable_topics:
            return None

        group_id = consumer_cls.build_config()["group.id"]

        return type[RetryConsumer](
            f"{consumer_cls.__name__}Retry",
            (cls, consumer_cls),
            {
                "topics": RetryTopics(group_id, *retryable_topics),
                "config": {
                    **getattr(cls, "config", {}),
                    "group.id": f"{group_id}.retry",
                },
            },
        )

    @classmethod
    def build_config(cls):
        return {
            **super().build_config(),
            **settings.RETRY_CONSUMER_CONFIG,
            **getattr(cls, "config", {}),
        }

    def retry_msg(self, msg: "cimpl.Message", exc: Exception) -> (bool, bool):
        retry_topic: RetryTopicConsumer = self.get_topic(msg)
        return retry_topic.get_producer_for(msg).retry(exc), False

    def dead_letter_msg(self, msg: "cimpl.Message", exc: Exception):
        retry_topic: RetryTopicConsumer = self.get_topic(msg)
        DeadLetterTopicProducer(group_id=retry_topic.group_id, msg=msg).produce_for(
            header_summary=str(exc),
            header_detail=traceback.format_exc(),
        )

    def process_message(self, msg: "cimpl.Message"):
        retry_time = RetryHeader.get_retry_time(msg.headers())
        if retry_time and retry_time > timezone.now():
            self.pause_partition(msg, retry_time)
            return
        super().process_message(msg)
