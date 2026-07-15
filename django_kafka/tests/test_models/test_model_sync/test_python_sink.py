from unittest import TestCase, mock

from django_kafka.models.model_sync import EnricherTransform
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.python import (
    PythonAvroSink,
    PythonSinkAvroTopicConsumer,
    Relation,
)
from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.topic import TopicConsumer

from .factories import (
    ModelWithFK,
    ModelWithFKChild,
    ModelWithNullableFK,
    RelatedModel,
    SimpleModel,
    make_sync,
)


@mock.patch(
    "django_kafka.conf.settings.MODEL_SYNC_CONSUMER",
    "django_kafka.consumer.Consumer",
)
class PythonSinkMakeTopicTestCase(TestCase):
    def _make_sync(self, **attrs):
        registry = ModelSyncRegistry()
        attrs.setdefault("source", None)
        attrs.setdefault("sink", PythonAvroSink())
        return make_sync(registry, **attrs)

    def test_creates_topic_consumer_instance(self):
        sync_cls = self._make_sync()
        topic = sync_cls().sink.make_topic()
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

    def test_auto_detects_fk_relations(self):
        sync_cls = self._make_sync(model=ModelWithFK)
        topic = sync_cls().sink.make_topic()
        self.assertEqual(len(topic.relations), 1)
        self.assertEqual(topic.relations[0].model, RelatedModel)
        self.assertEqual(topic.relations[0].id_field, "id")
        self.assertEqual(topic.relations[0].value_field, "related_id")

    def test_auto_detect_enables_resolver(self):
        sync_cls = self._make_sync(model=ModelWithFK)
        topic = sync_cls().sink.make_topic()
        self.assertTrue(topic.use_relations_resolver)

    def test_no_fk_model_has_no_relations_and_resolver_disabled(self):
        topic = self._make_sync()().sink.make_topic()
        self.assertEqual(topic.relations, [])
        self.assertFalse(topic.use_relations_resolver)

    def test_nullable_fk_excluded_from_auto_detection(self):
        sync_cls = self._make_sync(model=ModelWithNullableFK)
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relations, [])

    def test_explicit_relation_replaces_auto_detected(self):
        custom = Relation(
            RelatedModel,
            id_field="uuid",
            value_field="related_uuid",
            fk="related",
        )
        sync_cls = self._make_sync(
            model=ModelWithFK,
            sink=PythonAvroSink(relations=[custom]),
        )
        topic = sync_cls().sink.make_topic()
        self.assertEqual(len(topic.relations), 1)
        self.assertIs(topic.relations[0], custom)

    def test_explicit_relation_includes_nullable_fk(self):
        custom = Relation(
            RelatedModel,
            id_field="id",
            value_field="nullable_related_id",
            fk="nullable_related",
        )
        sync_cls = self._make_sync(
            model=ModelWithNullableFK,
            sink=PythonAvroSink(relations=[custom]),
        )
        topic = sync_cls().sink.make_topic()
        self.assertEqual(len(topic.relations), 1)
        self.assertIs(topic.relations[0], custom)

    def test_auto_detects_inherited_fk_from_mti_parent(self):
        sync_cls = self._make_sync(model=ModelWithFKChild)
        topic = sync_cls().sink.make_topic()
        relation_models = [r.model for r in topic.relations]
        self.assertIn(RelatedModel, relation_models)
        self.assertNotIn(ModelWithFK, relation_models)

    def _make_nullable_fk_topic(self):
        custom = Relation(
            RelatedModel,
            id_field="id",
            value_field="nullable_related_id",
            fk="nullable_related",
        )
        sync_cls = self._make_sync(
            model=ModelWithNullableFK,
            sink=PythonAvroSink(relations=[custom]),
        )
        return sync_cls().sink.make_topic()

    def _get_relations(self, topic, msg_value):
        with mock.patch.object(
            topic,
            "deserialize",
            side_effect=[{"id": 1}, msg_value],
        ):
            return list(topic.get_relations(mock.Mock()))

    def test_get_relations_yields_relation_for_value(self):
        topic = self._make_nullable_fk_topic()
        relations = self._get_relations(topic, {"nullable_related_id": 5})
        self.assertEqual(len(relations), 1)
        self.assertIsInstance(relations[0], ModelRelation)
        self.assertIs(relations[0].model, RelatedModel)
        self.assertEqual(relations[0].id_field, "id")
        self.assertEqual(relations[0].id_value, 5)

    def test_get_relations_skips_null_value(self):
        topic = self._make_nullable_fk_topic()
        relations = self._get_relations(topic, {"nullable_related_id": None})
        self.assertEqual(relations, [])

    def test_get_relations_skips_absent_value(self):
        topic = self._make_nullable_fk_topic()
        relations = self._get_relations(topic, {})
        self.assertEqual(relations, [])


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

    def test_consumer_path_raises_when_unconfigured(self):
        sink = PythonAvroSink()
        with (
            mock.patch("django_kafka.conf.settings.MODEL_SYNC_CONSUMER", None),
            self.assertRaises(ValueError),
        ):
            sink.consumer_path  # noqa: B018

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
