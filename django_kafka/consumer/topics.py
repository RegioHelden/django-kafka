from django_kafka.exceptions import TopicNotRegisteredError
from django_kafka.topic import TopicConsumer


class Topics:
    _topic_consumers: tuple["TopicConsumer", ...]
    _match: dict[str, "TopicConsumer"]

    def __init__(self, *topic_consumers: "TopicConsumer"):
        self._topic_consumers = topic_consumers
        self._match: dict[str, TopicConsumer] = {}

    def get(self, topic_name: str) -> "TopicConsumer":
        if topic_name not in self._match:
            topic_consumer = next((t for t in self if t.matches(topic_name)), None)
            if not topic_consumer:
                raise TopicNotRegisteredError(f"No topic registered for `{topic_name}`")
            self._match[topic_name] = topic_consumer

        return self._match[topic_name]

    def get_retryable(self, blocking=False) -> list["TopicConsumer"]:
        return [
            topic
            for topic in self._topic_consumers
            if topic.retry_settings and topic.retry_settings.blocking == blocking
        ]

    @property
    def names(self) -> list[str]:
        return [topic.name for topic in self]

    def __iter__(self):
        yield from self._topic_consumers

    def __bool__(self) -> bool:
        return bool(self.names)
