import logging
from pydoc import locate
from typing import Optional

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import cimpl

from django_kafka.conf import settings
from django_kafka.topic import Topic

logger = logging.getLogger(__name__)


class Topics(dict):
    def __init__(self, *topics: Topic):
        for topic in topics:
            self[topic.name] = topic


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

    topics: Topics[str, Topic]
    config: dict

    polling_freq = settings.POLLING_FREQUENCY
    default_logger = logger
    default_error_handler = settings.ERROR_HANDLER

    def __init__(self, config: Optional[dict] = None, **kwargs):
        kwargs.setdefault("logger", self.default_logger)
        kwargs.setdefault("error_cb", locate(self.default_error_handler)())

        self.config = {
            "client.id": settings.CLIENT_ID,
            **settings.GLOBAL_CONFIG,
            **settings.CONSUMER_CONFIG,
            **getattr(self, "config", {}),
            **(config or {}),
        }

        self._consumer = ConfluentConsumer(self.config, **kwargs)

    def __getattr__(self, name):
        """proxy consumer methods."""
        if name not in {"config"}:
            # For cases when `Consumer.config` is not set and
            #  `getattr(self, "config", {})` is called on `__init__`,
            #  the initialization will fail because `_consumer` is not yet set.
            return getattr(self._consumer, name)
        raise AttributeError(f"'{self.__class__.__name__}' has no attribute 'name'")

    def start(self):
        # define topics
        self.subscribe(topics=list(self.topics))
        while True:
            # poll every self.polling_freq seconds
            if msg := self.poll(timeout=self.polling_freq):
                self.process_message(msg)

    def stop(self):
        self.close()

    def process_message(self, msg: cimpl.Message):
        if msg_error := msg.error():
            self.handle_error(msg_error)
            return

        try:
            self.topics[msg.topic()].consume(msg)
        # ruff: noqa: BLE001 (we do not want consumer to stop if message processing is failing in any circumstances)
        except Exception as error:
            self.handle_error(error)
        else:
            self.commit_offset(msg)

    def commit_offset(self, msg: cimpl.Message):
        if not self.config.get("enable.auto.offset.store"):
            # Store the offset associated with msg to a local cache.
            # Stored offsets are committed to Kafka by a background
            #  thread every 'auto.commit.interval.ms'.
            # Explicitly storing offsets after processing gives at-least once semantics.
            self.store_offsets(msg)

    def handle_error(self, error):
        logger.error(error)
