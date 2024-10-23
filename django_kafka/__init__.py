import logging
from multiprocessing.pool import Pool
from typing import Optional, TYPE_CHECKING

from confluent_kafka.schema_registry import SchemaRegistryClient
from django.utils.functional import cached_property
from django.utils.module_loading import autodiscover_modules

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.producer import Producer
from django_kafka.registry import ConnectorsRegistry, ConsumersRegistry
from django_kafka.retry.settings import RetrySettings

if TYPE_CHECKING:
    from django_kafka.connect.connector import Connector
    from django_kafka.consumer import Consumer

logger = logging.getLogger(__name__)

__version__ = "0.5.4"

__all__ = [
    "autodiscover",
    "DjangoKafka",
    "kafka",
]


def autodiscover():
    autodiscover_modules(
        "consumers",
        "connectors",
        "kafka.consumers",
        "kafka.connectors",
    )


class DjangoKafka:
    connectors = ConnectorsRegistry()
    consumers = ConsumersRegistry()
    retry = RetrySettings

    @cached_property
    def producer(self) -> Producer:
        return Producer()

    @cached_property
    def schema_client(self) -> SchemaRegistryClient:
        """
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#schemaregistryclient
        """
        if not settings.SCHEMA_REGISTRY:
            raise DjangoKafkaError(
                "`SCHEMA_REGISTRY` configuration is not defined.",
            )

        return SchemaRegistryClient(settings.SCHEMA_REGISTRY)

    def run_consumer(self, consumer_key: str):
        consumer = self.consumers[consumer_key]()
        consumer.run()

    def run_consumers(self, consumers: Optional[list[str]] = None):
        consumers = consumers or list(self.consumers)
        with Pool(processes=len(consumers)) as pool:
            try:
                pool.map(self.run_consumer, consumers)
            except KeyboardInterrupt:
                # Stops the worker processes immediately without completing
                #  outstanding work.
                pool.terminate()
                # Wait for the worker processes to exit.
                # Should be called after close() or terminate().
                pool.join()
                logger.debug("KeyboardInterrupt. Pool workers terminated.")
            else:
                # Prevents any more tasks from being submitted to the pool.
                # Once all the tasks have been completed the worker processes will exit.
                pool.close()
                # Wait for the worker processes to exit.
                # Should be called after close() or terminate().
                pool.join()


kafka = DjangoKafka()
