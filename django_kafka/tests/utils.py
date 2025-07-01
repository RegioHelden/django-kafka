from unittest.mock import Mock

from faker import Faker

from django_kafka.utils.message import MessageTimestamp


def message_mock(  # noqa: PLR0913
    topic="topic",
    partition=0,
    offset=0,
    error=None,
    headers=None,
    timestamp: list[MessageTimestamp, int] | None = None,
):
    """mocking utility for confluent_kafka.cimpl.Message"""
    return Mock(
        **{
            "topic.return_value": topic,
            "partition.return_value": partition,
            "offset.return_value": offset,
            "headers.return_value": headers,
            "error.return_value": error,
            "key.return_value": Faker().binary(length=10),
            "timestamp.return_value": timestamp
            or (MessageTimestamp.NOT_AVAILABLE, Faker().unix_time() * 1000),
        },
    )


class AsyncIteratorMock(Mock):
    def __init__(self, items):
        super().__init__()
        self._iter = iter(items)
        self.return_value = self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as e:
            raise StopAsyncIteration from e
