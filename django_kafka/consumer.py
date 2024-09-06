import logging
import traceback
from pydoc import locate
from typing import TYPE_CHECKING, Optional

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import cimpl

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.topic import Topic

logger = logging.getLogger(__name__)


class Topics:
    _topics: tuple["Topic", ...]
    _match: dict[str, "Topic"]

    def __init__(self, *topics: "Topic"):
        self._topics = topics
        self._match: dict[str, "Topic"] = {}

    def get_topic(self, name: str) -> "Topic":
        if name not in self._match:
            topic = next((t for t in self if t.matches(name)), None)
            if not topic:
                raise DjangoKafkaError(f"No topic registered for `{name}`")
            self._match[name] = topic

        return self._match[name]

    @property
    def names(self) -> list[str]:
        return [topic.name for topic in self]

    def __iter__(self):
        yield from self._topics


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

    def retry_msg(self, msg: cimpl.Message, exc: Exception) -> bool:
        from django_kafka.retry.topic import RetryTopic

        topic = self.get_topic(msg)
        if not topic.retry_settings:
            return False

        return RetryTopic(group_id=self.group_id, main_topic=topic).retry_for(
            msg=msg,
            exc=exc,
        )

    def dead_letter_msg(self, msg: cimpl.Message, exc: Exception):
        from django_kafka.dead_letter.topic import DeadLetterTopic

        topic = self.get_topic(msg)
        DeadLetterTopic(group_id=self.group_id, main_topic=topic).produce_for(
            msg=msg,
            header_message=str(exc),
            header_detail=traceback.format_exc(),
        )

    def handle_exception(self, msg: cimpl.Message, exc: Exception):
        retried = self.retry_msg(msg, exc)
        if not retried:
            self.dead_letter_msg(msg, exc)
            self.log_error(exc)

    def get_topic(self, msg: cimpl.Message) -> "Topic":
        return self.topics.get_topic(name=msg.topic())

    def log_error(self, error):
        logger.error(error, exc_info=True)

    def process_message(self, msg: cimpl.Message):
        if msg_error := msg.error():
            self.log_error(msg_error)
            return

        try:
            self.get_topic(msg).consume(msg)
        # ruff: noqa: BLE001 (we do not want consumer to stop if message consumption fails in any circumstances)
        except Exception as exc:
            self.handle_exception(msg, exc)

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
                if msg := self.poll():
                    self.process_message(msg)
        except Exception as exc:
            self.log_error(exc)
            raise
        finally:
            self.stop()

    def stop(self):
        self.close()
