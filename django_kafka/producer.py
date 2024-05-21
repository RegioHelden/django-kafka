import logging
from pydoc import locate
from typing import Optional

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

    def __init__(self, config: Optional[dict] = None, **kwargs):
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
