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
    def to_datetime(cls, msg_timestamp: [int, int]) -> datetime | None:
        timestamp_type, timestamp = msg_timestamp

        if timestamp_type == MessageTimestamp.NOT_AVAILABLE:
            return None

        return datetime.fromtimestamp(timestamp / 1000, UTC)
