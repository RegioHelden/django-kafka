from abc import ABC, abstractmethod

from django_kafka.conf import settings
from django_kafka.models.model_sync.descriptor import DescriptorWrapper


class Source(DescriptorWrapper):
    """Base for all ModelSync sources."""


class ConnectorSource(ABC, Source):
    """Base for Kafka Connect based sources. Subclasses must implement setup."""

    connector: str | None = None

    @property
    def connector_path(self) -> str | None:
        return self.connector or getattr(settings, "MODEL_SYNC_SOURCE_CONNECTOR", None)

    @abstractmethod
    def setup(self, config: dict) -> None:
        """Mutate the connector config dict to include this model's source config."""
