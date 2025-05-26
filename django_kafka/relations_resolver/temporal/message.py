class Message:
    """
    This only exists because cimpl.Message is not instantiatable.
    https://github.com/confluentinc/confluent-kafka-python/issues/1535
    """

    def __init__(
        self,
        topic: str,
        key: bytes | str,
        value: bytes | str | None,
        headers: list[tuple[str, bytes]] | None,
    ):
        self._topic = topic
        self._key = key
        self._value = value
        self._headers = headers

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
