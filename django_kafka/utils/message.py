from datetime import UTC, datetime
from enum import IntEnum

MessageHeaders = list[tuple[str, str]]


class Header:
    @classmethod
    def list(cls, headers: MessageHeaders | None, header: str) -> list[str]:
        """returns all occurrences for the header"""
        if headers is not None:
            return [v for k, v in headers if k == header]
        return []

    @classmethod
    def get(cls, headers: MessageHeaders | None, header: str) -> str | None:
        """returns the first encountered value for the header"""
        if headers is not None:
            return next((v for k, v in headers if k == header), None)
        return None


class MessageTimestamp(IntEnum):
    """
    https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.timestamp
    Values are taken from the constants from `confluent_kafka.cimpl.py`
    """

    NOT_AVAILABLE = 0
    CREATE_TIME = 1
    LOG_APPEND_TIME = 2

    @classmethod
    def to_datetime(
        cls,
        msg_timestamp: tuple["MessageTimestamp", int],
    ) -> datetime | None:
        timestamp_type, timestamp = msg_timestamp

        if timestamp_type == MessageTimestamp.NOT_AVAILABLE:
            return None

        return datetime.fromtimestamp(timestamp / 1000, UTC)


class Message:
    """
    This only exists because cimpl.Message is not instantiatable.
    https://github.com/confluentinc/confluent-kafka-python/issues/1535
    """

    # ruff: noqa: PLR0913
    def __init__(
        self,
        topic: str,
        key: bytes | str,
        value: bytes | str | None,
        headers: list[tuple[str, bytes]] | None,
        offset: int,
        partition: int,
        timestamp: tuple[MessageTimestamp, int],
    ):
        self._topic = topic
        self._key = key
        self._value = value
        self._headers = headers
        self._offset = offset
        self._partition = partition
        self._timestamp = timestamp

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes | str:
        if type(self._key) is list:
            return bytes(self._key)
        return self._key

    def value(self) -> bytes | str:
        if type(self._value) is list:
            return bytes(self._value)
        return self._value

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def timestamp(self) -> tuple[MessageTimestamp, int] | None:
        return self._timestamp

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset
