import traceback
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Type, cast

from confluent_kafka import TopicPartition, cimpl
from django.utils import timezone

from django_kafka.conf import settings
from django_kafka.consumer import Consumer, Topics
from django_kafka.dead_letter.topic import DeadLetterTopic
from django_kafka.retry.headers import RetryHeader
from django_kafka.retry.topic import RetryTopic

if TYPE_CHECKING:
    from django_kafka.topic import Topic


class RetryTopics(Topics):
    def __init__(self, group_id: str, *topics: "Topic"):
        super().__init__(*(RetryTopic(group_id=group_id, main_topic=t) for t in topics))


class RetryConsumer(Consumer):
    topics: RetryTopics
    resume_times: dict[TopicPartition, datetime]

    def __init__(self):
        super().__init__()
        self.resume_times = {}

    @classmethod
    def build(cls, consumer_cls: Type["Consumer"]) -> Optional[Type["RetryConsumer"]]:
        """Generates RetryConsumer subclass linked to consumer class retryable topics"""
        retryable_topics = [t for t in consumer_cls.topics if t.retry_settings]
        if not retryable_topics:
            return None

        group_id = consumer_cls.build_config()["group.id"]

        return type[RetryConsumer](
            f"{consumer_cls.__name__}Retry",
            (cls,),
            {
                "config": {
                    **getattr(cls, "config", {}),
                    "group.id": f"{group_id}.retry",
                },
                "topics": RetryTopics(group_id, *retryable_topics),
            },
        )

    @classmethod
    def build_config(cls):
        return {
            **super().build_config(),
            **settings.RETRY_CONSUMER_CONFIG,
            **getattr(cls, "config", {}),
        }

    def retry_msg(self, msg: cimpl.Message, exc: Exception) -> bool:
        retry_topic = cast(RetryTopic, self.get_topic(msg))
        return retry_topic.retry_for(msg=msg, exc=exc)

    def dead_letter_msg(self, msg: cimpl.Message, exc: Exception):
        retry_topic = cast(RetryTopic, self.get_topic(msg))
        DeadLetterTopic(
            group_id=retry_topic.group_id,
            main_topic=retry_topic.main_topic,
        ).produce_for(
            msg=msg,
            header_message=str(exc),
            header_detail=traceback.format_exc(),
        )

    def pause_partition(self, msg, until: datetime):
        """pauses the partition and stores the resumption time"""
        tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
        self.seek(tp)  # seek back to message offset, so it is re-polled on unpause
        self.pause([tp])
        self.resume_times[tp] = until

    def resume_ready_partitions(self):
        """resumes any partitions that were paused"""
        now = timezone.now()
        for tp, until in list(self.resume_times.items()):
            if now < until:
                continue
            self.resume([tp])
            del self.resume_times[tp]

    def poll(self):
        self.resume_ready_partitions()
        return super().poll()

    def process_message(self, msg: cimpl.Message):
        retry_time = RetryHeader.get_retry_time(msg.headers())
        if retry_time and retry_time > timezone.now():
            self.pause_partition(msg, retry_time)
            return
        super().process_message(msg)

    def stop(self):
        self.resume_times = {}
        super().stop()
