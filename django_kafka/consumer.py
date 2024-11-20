import logging
import traceback
from datetime import datetime
from pydoc import locate
from typing import TYPE_CHECKING, Iterator, Optional

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import TopicPartition, cimpl
from django.utils import timezone

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.retry.settings import RetrySettings
    from django_kafka.topic import TopicConsumer

logger = logging.getLogger(__name__)


class Topics:
    _topic_consumers: tuple["TopicConsumer", ...]
    _match: dict[str, "TopicConsumer"]

    def __init__(self, *topic_consumers: "TopicConsumer"):
        self._topic_consumers = topic_consumers
        self._match: dict[str, TopicConsumer] = {}

    def get(self, topic_name: str) -> "TopicConsumer":
        if topic_name not in self._match:
            topic_consumer = next((t for t in self if t.matches(topic_name)), None)
            if not topic_consumer:
                raise DjangoKafkaError(f"No topic registered for `{topic_name}`")
            self._match[topic_name] = topic_consumer

        return self._match[topic_name]

    def get_retryable(self, blocking=False) -> list["TopicConsumer"]:
        return [
            topic
            for topic in self._topic_consumers
            if topic.retry_settings and topic.retry_settings.blocking == blocking
        ]

    @property
    def names(self) -> list[str]:
        return [topic.name for topic in self]

    def __iter__(self):
        yield from self._topic_consumers


class RetryManager:
    """Manager for blocking message retry attempts"""

    __retries: dict[TopicPartition, int]

    def __init__(self):
        self.__retries = {}

    @staticmethod
    def get_msg_partition(msg: cimpl.Message) -> TopicPartition:
        """returns the message topic partition, set to the message offset

        Note: TopicPartition hashes based on topic/partition, but not the offset.
        """
        return TopicPartition(msg.topic(), msg.partition(), msg.offset())

    def next(self, msg: cimpl.Message):
        """increments and returns the partition attempt count"""
        msg_tp = self.get_msg_partition(msg)
        for tp in self.__retries:
            if tp == msg_tp and tp.offset != msg_tp.offset:
                del self.__retries[tp]  # new offset encountered, reset entry
                break

        next_attempt = self.__retries.get(msg_tp, 0) + 1
        self.__retries[msg_tp] = next_attempt
        return next_attempt

    def reset(self):
        self.__retries = {}


class PauseManager:
    """Manager for partition pauses"""

    __pauses: dict[TopicPartition, datetime]

    def __init__(self):
        self.__pauses = {}

    @staticmethod
    def get_msg_partition(msg: cimpl.Message) -> TopicPartition:
        """returns the message topic partition"""
        return TopicPartition(msg.topic(), msg.partition())

    def set(self, msg: cimpl.Message, until: datetime) -> TopicPartition:
        """adds message partition to the pause list, returning the partition"""
        tp = self.get_msg_partition(msg)
        self.__pauses[tp] = until
        return tp

    def pop_ready(self) -> Iterator[TopicPartition]:
        """returns the partitions ready to resume, removing them from the pause list"""
        now = timezone.now()
        for tp, pause in list(self.__pauses.items()):
            if now >= pause:
                yield tp
                del self.__pauses[tp]

    def reset(self):
        self.__pauses = {}


class Consumer:
    """
    Available settings of the producers (P) and consumers (C)
        https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    Consumer configs
        https://kafka.apache.org/documentation/#consumerconfigs
    Kafka Client Configuration
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    confluent_kafka.Consumer API
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer
    """

    topics: Topics
    config: dict

    polling_freq = settings.POLLING_FREQUENCY
    default_logger = logger
    default_error_handler = settings.ERROR_HANDLER

    def __init__(self):
        self.config = self.build_config()
        self._consumer = ConfluentConsumer(self.config)
        self._retries = RetryManager()  # blocking retry manager
        self._pauses = PauseManager()

    def __getattr__(self, name):
        """proxy consumer methods."""
        return getattr(self._consumer, name)

    @classmethod
    def build_config(cls):
        return {
            "client.id": settings.CLIENT_ID,
            **settings.GLOBAL_CONFIG,
            **settings.CONSUMER_CONFIG,
            "logger": cls.default_logger,
            "error_cb": locate(cls.default_error_handler)(),
            **getattr(cls, "config", {}),
        }

    @property
    def group_id(self) -> str:
        return self.config["group.id"]

    def commit_offset(self, msg: cimpl.Message):
        if not self.config.get("enable.auto.offset.store"):
            # Store the offset associated with msg to a local cache.
            # Stored offsets are committed to Kafka by a background
            #  thread every 'auto.commit.interval.ms'.
            # Explicitly storing offsets after processing gives at-least once semantics.
            self.store_offsets(msg)

    def pause_partition(self, msg: "cimpl.Message", until: datetime):
        """pauses message partition to process the message at a later time

        note: pausing is only retained within the python consumer class, and is not
        retained between separate consumer processes.
        """
        partition = self._pauses.set(msg, until)
        self.seek(partition)  # seek back to message offset to re-poll on unpause
        self.pause([partition])

    def resume_partitions(self):
        """resumes any paused partitions that are now ready"""
        for partition in self._pauses.pop_ready():
            self.resume([partition])

    def blocking_retry(
        self,
        retry_settings: "RetrySettings",
        msg: cimpl.Message,
        exc: Exception,
    ) -> bool:
        """
        blocking retry, managed within the same consumer process

        :return: whether the message will be retried
        """
        attempt = self._retries.next(msg)
        if retry_settings.can_retry(attempt, exc):
            until = retry_settings.get_retry_time(attempt)
            self.pause_partition(msg, until)
            self.log_error(exc)
            return True
        return False

    def non_blocking_retry(
        self,
        retry_settings: "RetrySettings",
        msg: cimpl.Message,
        exc: Exception,
    ):
        """
        non-blocking retry, managed by a separate topic and consumer process

        :return: whether the message will be retried
        """
        from django_kafka.retry.topic import RetryTopicProducer

        return RetryTopicProducer(
            retry_settings=retry_settings,
            group_id=self.group_id,
            msg=msg,
        ).retry(exc=exc)

    def retry_msg(self, msg: cimpl.Message, exc: Exception) -> (bool, bool):
        """
        :return tuple: The first element indicates if the message was retried and the
        second indicates if the retry was blocking.
        """
        retry_settings = self.get_topic(msg).retry_settings
        if not retry_settings:
            return False, False
        if retry_settings.blocking:
            return self.blocking_retry(retry_settings, msg, exc), True
        return self.non_blocking_retry(retry_settings, msg, exc), False

    def dead_letter_msg(self, msg: cimpl.Message, exc: Exception):
        """publishes a message to the dead letter topic, with exception details"""
        from django_kafka.dead_letter.topic import DeadLetterTopicProducer

        DeadLetterTopicProducer(group_id=self.group_id, msg=msg).produce_for(
            header_message=str(exc),
            header_detail=traceback.format_exc(),
        )

    def handle_exception(self, msg: cimpl.Message, exc: Exception) -> bool:
        """
        return bool: Indicates if the message was processed and offset can be committed.
        """
        retried, blocking = self.retry_msg(msg, exc)
        if not retried:
            self.dead_letter_msg(msg, exc)
            self.log_error(exc)
            return True
        return not blocking

    def get_topic(self, msg: cimpl.Message) -> "TopicConsumer":
        return self.topics.get(topic_name=msg.topic())

    def log_error(self, error):
        logger.error(error, exc_info=True)

    def consume(self, msg):
        self.get_topic(msg).consume(msg)

    def process_message(self, msg: cimpl.Message):
        if msg_error := msg.error():
            self.log_error(msg_error)
            return

        try:
            self.consume(msg)
        except Exception as exc:
            # ruff: noqa: BLE001 (do not stop consumer if message consumption fails in any circumstances)
            processed = self.handle_exception(msg, exc)
        else:
            processed = True

        if processed:
            self.commit_offset(msg)

    def poll(self) -> Optional[cimpl.Message]:
        # poll for self.polling_freq seconds
        return self._consumer.poll(timeout=self.polling_freq)

    def start(self):
        self.subscribe(topics=self.topics.names)

    def run(self):
        try:
            self.start()
            while True:
                self.resume_partitions()
                if (msg := self.poll()) is not None:
                    self.process_message(msg)
        except Exception as exc:
            self.log_error(exc)
            raise
        finally:
            self.stop()

    def stop(self):
        self.close()
