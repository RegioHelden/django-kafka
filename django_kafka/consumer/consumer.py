import logging
import traceback
from datetime import datetime, timedelta
from pydoc import locate
from typing import TYPE_CHECKING, Optional

from confluent_kafka import Consumer as ConfluentConsumer
from django.utils import timezone

from django_kafka import kafka
from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

from .managers import PauseManager, RetryManager

if TYPE_CHECKING:
    from confluent_kafka import TopicPartition, cimpl

    from django_kafka.consumer import Topics
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

    topics: "Topics"
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

    @staticmethod
    def debug_log(
        description: str,
        msg: "cimpl.Message" = None,
        partition: "TopicPartition" = None,
    ):
        if msg is not None:
            logger.debug(
                "%s - topic='%s' partition=%d offset=%d",
                description,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
        elif partition is not None:
            logger.debug(
                "%s - topic='%s' partition=%d offset=%d",
                description,
                partition.topic,
                partition.partition,
                partition.offset,
            )
        else:
            logger.debug(description)

    def commit_offset(self, msg: "cimpl.Message"):
        if not self.config.get("enable.auto.offset.store"):
            # Store the offset associated with msg to a local cache.
            # Stored offsets are committed to Kafka by a background
            #  thread every 'auto.commit.interval.ms'.
            # Explicitly storing offsets after processing gives at-least once semantics.
            self.debug_log("Commit offset", msg=msg)
            self.store_offsets(msg)

    def pause_partition(self, msg: "cimpl.Message", until: datetime):
        """pauses message partition to process the message at a later time

        note: pausing is only retained within the python consumer class, and is not
        retained between separate consumer processes.
        """
        self.debug_log("Pause partition", msg=msg)
        partition = self._pauses.set(msg, until)
        self.seek(partition)  # seek back to message offset to re-poll on unpause
        self.pause([partition])

    def resume_partitions(self):
        """resumes any paused partitions that are now ready"""
        for partition in self._pauses.pop_ready():
            self.debug_log("Resume partition", partition=partition)
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
        if retry_settings.should_retry(msg, attempt, exc):
            until = retry_settings.get_retry_time(attempt)
            self.pause_partition(msg, until)

            if retry_settings.should_log(attempt):
                self.log_error(msg, exc_info=exc)

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

        topic = RetryTopicProducer(
            retry_settings=retry_settings,
            group_id=self.group_id,
            msg=msg,
        )

        retried = topic.retry(exc=exc)

        if retried and retry_settings.should_log(topic.retry_attempt):
            self.log_error(msg, exc_info=exc)

        return retried

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
            header_summary=str(exc),
            header_detail=traceback.format_exc(),
        )

    def handle_exception(self, msg: "cimpl.Message", exc: Exception) -> bool:
        """
        return bool: Indicates if the message was processed and offset can be committed.
        """
        retried, blocking = self.retry_msg(msg, exc)
        if not retried:
            self.dead_letter_msg(msg, exc)
            self.log_error(msg, exc_info=exc)
            return True
        return not blocking

    def get_topic(self, msg: "cimpl.Message") -> "TopicConsumer":
        return self.topics.get(topic_name=msg.topic())

    def log_error(
        self,
        msg: Optional["cimpl.Message"] = None,
        exc_info: bool | Exception = False,
    ):
        error = f"'{self.__class__.__module__}.{self.__class__.__name__} failed'"
        if msg:
            topic = self.get_topic(msg)
            error = (
                f"{error} on '{topic.__class__.__module__}.{topic.__class__.__name__}'"
            )

            if msg_error := msg.error():
                error = f"{error}\nMessage error: '{msg_error}'"

        logger.error(error, exc_info=exc_info)

    def consume(self, msg) -> bool:
        """
        return value tells if the offset should be committed.
        """
        self.debug_log("Consume message", msg=msg)
        topic = self.get_topic(msg)

        if topic.use_relations_resolver:
            return self._resolve_relations(msg, topic)

        topic.consume(msg)
        return True

    def _resolve_relations(self, msg, topic) -> bool:
        resolve_action = kafka.relations_resolver.resolve(topic.get_relations(msg), msg)

        # 1. relation resolver didn't find anything to handle
        # - consume and commit
        if resolve_action == kafka.relations_resolver.Action.CONTINUE:
            topic.consume(msg)
            return True

        # 2. relation does not exist, the message was sent to waiting queue
        # - commit offset without consumption and keep processing messages
        if resolve_action == kafka.relations_resolver.Action.SKIP:
            return True

        # 3 relation exists but there are messages waiting to be resolved,
        # - stop consumption until all waiting messages are resolved,
        #   don't commit offset
        if resolve_action == kafka.relations_resolver.Action.PAUSE:
            self.pause_partition(msg, timezone.now() + timedelta(seconds=10))
            return False

        raise DjangoKafkaError(
            f"'RelationResolver.Action({resolve_action})' case is not implemented.",
        )

    def process_message(self, msg: "cimpl.Message"):
        if msg.error():
            self.log_error(msg)
            return

        try:
            commit = self.consume(msg)
        except Exception as exc:
            # ruff: noqa: BLE001 (do not stop consumer if message consumption fails in any circumstances)
            commit = self.handle_exception(msg, exc)

        if commit is None:
            raise DjangoKafkaError(
                "'Consumer.consume' must return 'bool'."
                " Did you call 'super().consume()' without return?",
            )

        if commit:
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
            self.log_error(exc_info=exc)
            raise
        finally:
            self.stop()

    def stop(self):
        self.close()
