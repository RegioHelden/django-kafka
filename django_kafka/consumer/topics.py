from django_kafka.exceptions import TopicNotRegisteredError
from django_kafka.topic import TopicConsumer


class Topics:
    """
    A collection of TopicConsumers attached to a Consumer class.

    Iteration yields:
    1. The explicit `TopicConsumer` instances passed to `__init__`.
    2. Topics auto-registered for the owning Consumer class via the model_sync
       registry. Every `ModelSync` with a `PythonSink` or `enrich_transforms`
       contributes a topic here, so users don't need to declare them by hand.

    Standalone instances (not attached to a Consumer class) yield only the
    explicit topics, since model_sync topics are looked up by owner.
    """

    _topic_consumers: tuple["TopicConsumer", ...]
    _match: dict[str, "TopicConsumer"]
    _owner: type | None

    def __init__(self, *topic_consumers: "TopicConsumer"):
        self._topic_consumers = topic_consumers
        self._match: dict[str, TopicConsumer] = {}
        self._owner = None

    def __set_name__(self, owner, name):
        # Python calls __set_name__ when the class body finishes executing,
        # passing the owning class. We capture it so iteration can look up
        # model_sync-registered topics keyed by this Consumer class.
        self._owner = owner

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
        if self._owner is not None:
            # ruff: noqa: PLC0415
            from django_kafka.models.model_sync.registry import model_sync_registry

            yield from model_sync_registry.get_topics_for_consumer_class(self._owner)

    def __bool__(self) -> bool:
        return bool(self.names)
