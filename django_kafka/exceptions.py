from typing import Any


class DjangoKafkaError(Exception):
    def __init__(self, *args, context: Any | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context


class TopicNotRegisteredError(DjangoKafkaError):
    pass
