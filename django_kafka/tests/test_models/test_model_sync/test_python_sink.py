from unittest import TestCase, mock

from django_kafka.models.model_sync import EnricherTransform
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.python import (
    PythonAvroSink,
    PythonSinkAvroTopicConsumer,
    Relation,
)
from django_kafka.topic import TopicConsumer
from django_kafka.topic.model import ModelTopicConsumer

from .factories import SimpleModel, make_sync


class PythonSinkMakeTopicTestCase(TestCase):
    def _make_sync(self, **attrs):
        registry = ModelSyncRegistry()
        attrs.setdefault("source", None)
        attrs.setdefault("sink", PythonAvroSink())
        return make_sync(registry, **attrs)

    def test_creates_topic_consumer_instance(self):
        sync_cls = self._make_sync()
        topic = sync_cls().sink.make_topic()
        self.assertIsInstance(topic, ModelTopicConsumer)
        self.assertIsInstance(topic, PythonSinkAvroTopicConsumer)

    def test_topic_name_is_source_topic_without_enrich(self):
        sync_cls = self._make_sync()
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.name, sync_cls.source_topic())

    def test_topic_name_is_enriched_topic_with_enrich(self):
        sync_cls = self._make_sync(enrich_transforms=[EnricherTransform()])
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.name, sync_cls.get_enriched_topic())

    def test_default_is_deletion_provided(self):
        topic = self._make_sync()().sink.make_topic()
        self.assertTrue(topic.is_deletion(SimpleModel, {}, None))
        self.assertFalse(topic.is_deletion(SimpleModel, {}, {"name": "test"}))

    def test_default_is_deletion_handles_deleted_flag(self):
        topic = self._make_sync()().sink.make_topic()
        self.assertTrue(topic.is_deletion(SimpleModel, {}, {"__deleted": "true"}))
        self.assertFalse(topic.is_deletion(SimpleModel, {}, {"__deleted": "false"}))

    def test_default_get_lookup_kwargs_uses_key_fields(self):
        topic = self._make_sync()().sink.make_topic()
        result = topic.get_lookup_kwargs(SimpleModel, {"kafka_uuid": "abc"}, {})
        self.assertEqual(result, {"kafka_uuid": "abc"})

    def test_relations_enables_resolver(self):
        sync_cls = self._make_sync(
            sink=PythonAvroSink(
                relations=[
                    Relation(
                        SimpleModel,
                        id_field="id",
                        value_field="model_id",
                        fk="related",
                    ),
                ],
            ),
        )
        topic = sync_cls().sink.make_topic()
        self.assertTrue(topic.use_relations_resolver)

    def test_no_relations_resolver_disabled(self):
        topic = self._make_sync()().sink.make_topic()
        self.assertFalse(topic.use_relations_resolver)


class PythonSinkConsumerPathTestCase(TestCase):
    def test_consumer_path_from_init(self):
        sink = PythonAvroSink(consumer="myapp.consumers.MyConsumer")
        self.assertEqual(sink.consumer_path, "myapp.consumers.MyConsumer")

    def test_consumer_path_falls_back_to_setting(self):
        sink = PythonAvroSink()
        with mock.patch(
            "django_kafka.conf.settings.MODEL_SYNC_CONSUMER",
            "myapp.consumers.Default",
        ):
            self.assertEqual(sink.consumer_path, "myapp.consumers.Default")

    def test_topic_consumer_class_default(self):
        self.assertEqual(
            PythonAvroSink.topic_consumer_class,
            PythonSinkAvroTopicConsumer,
        )

    def test_topic_consumer_class_override_via_init(self):
        class CustomConsumer(TopicConsumer):
            pass

        sink = PythonAvroSink(topic_consumer_class=CustomConsumer)
        self.assertEqual(sink.topic_consumer_class, CustomConsumer)
