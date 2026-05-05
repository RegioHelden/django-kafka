from abc import ABC, abstractmethod

from django_kafka.models.model_sync.descriptor import DescriptorWrapper


class Sink(DescriptorWrapper):
    """Base for all ModelSync sinks."""


class ConnectorSink(ABC, Sink):
    """Base for Kafka Connect based sinks."""

    @property
    @abstractmethod
    def config(self) -> dict:
        """Full standalone connector configuration for this sink."""
