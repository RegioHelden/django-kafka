import logging
import traceback
from datetime import datetime
from pydoc import locate
from typing import TYPE_CHECKING, Optional

from confluent_kafka import Consumer as ConfluentConsumer

from django_kafka.conf import settings

from .managers import PauseManager, RetryManager
from .topics import Topics

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.retry.settings import RetrySettings
    from django_kafka.topic import TopicConsumer

logger = logging.getLogger(__name__)


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

    def commit_offset(self, msg: "cimpl.Message"):
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
        msg: "cimpl.Message",
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
        msg: "cimpl.Message",
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

    def retry_msg(self, msg: "cimpl.Message", exc: Exception) -> (bool, bool):
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

    def dead_letter_msg(self, msg: "cimpl.Message", exc: Exception):
        """publishes a message to the dead letter topic, with exception details"""
        from django_kafka.dead_letter.topic import DeadLetterTopicProducer

        DeadLetterTopicProducer(group_id=self.group_id, msg=msg).produce_for(
            header_message=str(exc),
            header_detail=traceback.format_exc(),
        )

    def handle_exception(self, msg: "cimpl.Message", exc: Exception) -> bool:
        """
        return bool: Indicates if the message was processed and offset can be committed.
        """
        retried, blocking = self.retry_msg(msg, exc)
        if not retried:
            self.dead_letter_msg(msg, exc)
            self.log_error(exc)
            return True
        return not blocking

    def get_topic(self, msg: "cimpl.Message") -> "TopicConsumer":
        return self.topics.get(topic_name=msg.topic())

    def log_error(self, error):
        logger.error(error, exc_info=True)

    def consume(self, msg):
        self.get_topic(msg).consume(msg)

    def process_message(self, msg: "cimpl.Message"):
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

    def poll(self) -> Optional["cimpl.Message"]:
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
