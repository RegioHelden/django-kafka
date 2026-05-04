from unittest import TestCase

from django_kafka.consumer import Consumer
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.python import PythonAvroSink

from .factories import make_sync


class _SinkConsumer(Consumer):
    """Standalone Consumer used as the registration target in tests."""

    config = {"group.id": "test-sink"}


class EnsureConsumerRegisteredTestCase(TestCase):
    def test_raises_on_invalid_path(self):
        registry = ModelSyncRegistry()
        with self.assertRaises(ValueError):
            registry._ensure_consumer_registered("nonexistent.Consumer")


class GetTopicsForConsumerTestCase(TestCase):
    def test_yields_python_sink_topic(self):
        registry = ModelSyncRegistry()
        consumer_path = f"{_SinkConsumer.__module__}.{_SinkConsumer.__name__}"
        make_sync(
            registry,
            source=None,
            sink=PythonAvroSink(consumer=consumer_path),
        )
        topics = list(registry.get_topics_for_consumer_class(_SinkConsumer))
        self.assertEqual(len(topics), 1)

    def test_yields_nothing_for_non_matching_consumer(self):
        registry = ModelSyncRegistry()
        consumer_path = f"{_SinkConsumer.__module__}.{_SinkConsumer.__name__}"
        make_sync(
            registry,
            source=None,
            sink=PythonAvroSink(consumer=consumer_path),
        )

        class OtherConsumer(Consumer):
            config = {"group.id": "other"}

        topics = list(registry.get_topics_for_consumer_class(OtherConsumer))
        self.assertEqual(len(topics), 0)
