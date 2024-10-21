import traceback
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Type, cast

from confluent_kafka import TopicPartition, cimpl
from django.utils import timezone

from django_kafka.conf import settings
from django_kafka.consumer import Consumer, Topics
from django_kafka.dead_letter.topic import DeadLetterTopicProducer
from django_kafka.retry.header import RetryHeader
from django_kafka.retry.topic import RetryTopicConsumer

if TYPE_CHECKING:
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
    resume_times: dict[TopicPartition, datetime]

    def __init__(self):
        super().__init__()
        self.resume_times = {}

    @classmethod
    def build(cls, consumer_cls: Type["Consumer"]) -> Optional[Type["RetryConsumer"]]:
        """Generates RetryConsumer subclass linked to consumer class retryable topics"""
        retryable_tcs = [t for t in consumer_cls.topics if t.retry_settings]
        if not retryable_tcs:
            return None

        group_id = consumer_cls.build_config()["group.id"]

        return type[RetryConsumer](
            f"{consumer_cls.__name__}Retry",
            (cls, consumer_cls),
            {
                "topics": RetryTopics(group_id, *retryable_tcs),
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

    def retry_msg(self, msg: cimpl.Message, exc: Exception) -> bool:
        rt_consumer = cast(RetryTopicConsumer, self.get_topic_consumer(msg))
        return rt_consumer.producer_for(msg).retry(exc)

    def dead_letter_msg(self, msg: cimpl.Message, exc: Exception):
        rt_consumer = cast(RetryTopicConsumer, self.get_topic_consumer(msg))
        DeadLetterTopicProducer(group_id=rt_consumer.group_id, msg=msg).produce_for(
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
