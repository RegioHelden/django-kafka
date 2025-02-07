from abc import ABC, abstractmethod
from enum import StrEnum
from http import HTTPStatus

from django_kafka.conf import settings
from django_kafka.connect.client import KafkaConnectClient
from django_kafka.exceptions import DjangoKafkaError

__all__ = [
    "Connector",
    "ConnectorStatus",
]


class ConnectorStatus(StrEnum):
    """
    https://docs.confluent.io/platform/current/connect/monitoring.html#connector-and-task-status
    UNASSIGNED: The connector/task has not yet been assigned to a worker.
    RUNNING: The connector/task is running.
    PAUSED: The connector/task has been administratively paused.
    FAILED: The connector/task has failed (usually by raising an exception, which
            is reported in the status output).
    """

    UNASSIGNED = "UNASSIGNED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"


class Name:
    def __get__(self, instance, owner):
        if settings.CONNECTOR_NAME_PREFIX:
            return f"{settings.CONNECTOR_NAME_PREFIX}.{owner.__name__}"
        return owner.__name__


class Connector(ABC):
    name = Name()
    mark_for_removal = False

    @property
    @abstractmethod
    def config(self) -> dict:
        """Configurations for the connector."""

    def __init__(self):
        if not settings.CONNECT_HOST:
            raise DjangoKafkaError("Kafka `CONNECT_HOST` is not configured.")

        self.client = KafkaConnectClient(
            host=settings.CONNECT_HOST,
            auth=settings.CONNECT_AUTH,
            retry=settings.CONNECT_RETRY,
            timeout=settings.CONNECT_REQUESTS_TIMEOUT,
        )

    def delete(self) -> bool:
        response = self.client.delete(self.name)

        if response.status_code == HTTPStatus.NOT_FOUND:
            return False

        if not response.ok:
            raise DjangoKafkaError(response.text, context=response)

        return True

    def submit(self) -> dict:
        response = self.client.update_or_create(self.name, self.config)

        if not response.ok:
            raise DjangoKafkaError(response.text, context=response)

        return response.json()

    def is_valid(self, raise_exception=False) -> bool:
        response = self.client.validate(self.config)

        if raise_exception and not response.ok:
            raise DjangoKafkaError(response.text, context=response)

        return response.ok

    def status(self) -> ConnectorStatus:
        response = self.client.connector_status(self.name)

        if not response.ok:
            raise DjangoKafkaError(response.text, context=response)
        return response.json()["connector"]["state"]
