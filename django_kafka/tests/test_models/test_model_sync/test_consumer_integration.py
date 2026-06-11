from typing import ClassVar
from unittest import TestCase

from django_kafka.consumer import Consumer
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.python import (
    PythonAvroSink,
    PythonSinkAvroTopicConsumer,
)

from .factories import make_sync


class _SinkConsumer(Consumer):
    """Standalone Consumer used as the registration target in tests."""

    config: ClassVar = {"group.id": "test-sink"}


class EnsureConsumerRegisteredTestCase(TestCase):
    def test_raises_on_invalid_path(self):
        registry = ModelSyncRegistry()
        with self.assertRaises(ValueError):
            registry._ensure_consumer_registered("nonexistent.Consumer")


class GetTopicsForConsumerTestCase(TestCase):
    def setUp(self):
        self.registry = ModelSyncRegistry()
        consumer_path = f"{_SinkConsumer.__module__}.{_SinkConsumer.__name__}"
        make_sync(
            self.registry,
            source=None,
            sink=PythonAvroSink(consumer=consumer_path),
        )

    def test_yields_python_sink_topic(self):
        topics = list(self.registry.get_topics_for_consumer_class(_SinkConsumer))
        self.assertEqual(len(topics), 1)
        self.assertIsInstance(topics[0], PythonSinkAvroTopicConsumer)

    def test_yields_nothing_for_non_matching_consumer(self):
        class OtherConsumer(Consumer):
            config: ClassVar = {"group.id": "other"}

        topics = list(self.registry.get_topics_for_consumer_class(OtherConsumer))
        self.assertEqual(len(topics), 0)
