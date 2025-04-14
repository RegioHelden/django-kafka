import logging
from collections.abc import Callable
from contextlib import ContextDecorator
from contextvars import ContextVar
from pydoc import locate

from confluent_kafka import Producer as ConfluentProducer

from django_kafka.conf import settings

logger = logging.getLogger(__name__)


class Producer:
    """
    Available settings of the producers (P) and consumers (C):
        https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    Producer configs
        https://kafka.apache.org/documentation/#producerconfigs
    Kafka Client Configuration
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    confluent_kafka.Producer API
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-producer
    """

    config: dict

    default_logger = logger
    default_error_handler = settings.ERROR_HANDLER

    def __init__(self, config: dict | None = None, **kwargs):
        kwargs.setdefault("logger", self.default_logger)
        kwargs.setdefault("error_cb", locate(self.default_error_handler)())

        self._producer = ConfluentProducer(
            {
                "client.id": settings.CLIENT_ID,
                **settings.GLOBAL_CONFIG,
                **settings.PRODUCER_CONFIG,
                **getattr(self, "config", {}),
                **(config or {}),
            },
            **kwargs,
        )

    def produce(self, name, *args, **kwargs):
        if not Suppression.active(name):
            self._producer.produce(name, *args, **kwargs)

    def __getattr__(self, name):
        """
        proxy producer methods.
        """
        if name not in {"config"}:
            # For cases when `Producer.config` is not set and
            #  `getattr(self, "config", {})` is called on `__init__`,
            #  the initialization will fail because `_consumer` is not yet set.
            return getattr(self._producer, name)
        raise AttributeError(f"'{self.__class__.__name__}' has no attribute 'name'")


class Suppression(ContextDecorator):
    """context manager to help suppress producing messages to desired Kafka topics"""

    _var = ContextVar(f"{__name__}.suppression")

    @classmethod
    def active(cls, topic: str):
        """returns if suppression is enabled for the given topic"""
        try:
            topics = cls._var.get()
        except LookupError:
            # context var is not yet set, defaulting to empty list
            topics = []

        # topics will be None when suppress() is initialized without topics provided
        if topics is None:
            return True  # all topics

        return topic in topics

    def __init__(self, topics: list[str] | None = None, deactivate=False):
        try:
            topics_in_context = self._var.get()
        except LookupError:
            topics_in_context = []

        if deactivate:
            self.topics = []

        elif topics is None or topics_in_context is None:
            self.topics = None  # indicates all topics

        elif isinstance(topics, list):
            self.topics = topics_in_context + topics

        else:
            raise ValueError(f"invalid producer suppression setting {topics}")

    def __enter__(self):
        self.token = self._var.set(self.topics)
        return self

    def __exit__(self, *args, **kwargs):
        self._var.reset(self.token)


def suppress(topics: Callable | list[str] | None = None):
    if callable(topics):
        return Suppression()(topics)
    return Suppression(topics)


def unsuppress(fn: Callable | None = None):
    if fn:
        return Suppression(deactivate=True)(fn)
    return Suppression(deactivate=True)
