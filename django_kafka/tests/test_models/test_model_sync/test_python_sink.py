from unittest import TestCase, mock

from django.contrib.contenttypes.models import ContentType
from django.db import models

from django_kafka.models.model_sync import (
    EnricherTransform,
    RelationTransform,
    StaticValueTransform,
)
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.python import (
    PythonAvroSink,
    PythonSinkAvroTopicConsumer,
)
from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.tests.models import AbstractModelTestCase
from django_kafka.topic import TopicConsumer

from .factories import (
    ModelWithContentTypeFK,
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
        self.assertEqual(len(topic.relation_transforms), 1)
        self.assertEqual(topic.relation_transforms[0].model, RelatedModel)
        self.assertEqual(topic.relation_transforms[0].id_field, "id")
        self.assertEqual(topic.relation_transforms[0].source, "related_id")
        self.assertIsNone(topic.relation_transforms[0].target)

    def test_auto_detected_relation_writes_the_id_without_a_lookup(self):
        sync_cls = self._make_sync(model=ModelWithFK)
        topic = sync_cls().sink.make_topic()

        with mock.patch.object(RelatedModel, "objects") as objects:
            result = topic.transform(ModelWithFK, {"related_id": 7})

        objects.get.assert_not_called()
        self.assertEqual(result["related_id"], 7)

    def test_auto_detected_relation_leaves_an_absent_id_absent(self):
        sync_cls = self._make_sync(model=ModelWithFK)
        topic = sync_cls().sink.make_topic()

        result = topic.transform(ModelWithFK, {"name": "x"})

        self.assertNotIn("related_id", result)

    def test_declared_wait_only_relation_suppresses_auto_detection(self):
        custom = RelationTransform(
            source="related_id",
            model=RelatedModel,
            id_field="uuid",
        )
        sync_cls = self._make_sync(model=ModelWithFK, consume_transforms=[custom])
        topic = sync_cls().sink.make_topic()

        self.assertEqual(topic.relation_transforms, [custom])

    def test_auto_detect_enables_resolver(self):
        sync_cls = self._make_sync(model=ModelWithFK)
        topic = sync_cls().sink.make_topic()
        self.assertTrue(topic.use_relations_resolver)

    def test_no_fk_model_has_no_relations_and_resolver_disabled(self):
        topic = self._make_sync()().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [])
        self.assertFalse(topic.use_relations_resolver)

    def test_content_type_fk_excluded_from_auto_detection(self):
        sync_cls = self._make_sync(model=ModelWithContentTypeFK)
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [])

    def test_declared_content_type_relation_is_kept(self):
        custom = RelationTransform(source="content_type_id", model=ContentType)
        sync_cls = self._make_sync(
            model=ModelWithContentTypeFK,
            consume_transforms=[custom],
        )
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [custom])

    def test_nullable_fk_excluded_from_auto_detection(self):
        sync_cls = self._make_sync(model=ModelWithNullableFK)
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [])

    def test_declared_relation_replaces_auto_detected(self):
        custom = RelationTransform(
            source="related_uuid",
            target="related",
            model=RelatedModel,
            id_field="uuid",
        )
        sync_cls = self._make_sync(model=ModelWithFK, consume_transforms=[custom])
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [custom])

    def test_declared_relation_includes_nullable_fk(self):
        custom = self._nullable_fk_transform()
        sync_cls = self._make_sync(
            model=ModelWithNullableFK,
            consume_transforms=[custom],
        )
        topic = sync_cls().sink.make_topic()
        self.assertEqual(topic.relation_transforms, [custom])

    def test_auto_detects_inherited_fk_from_mti_parent(self):
        sync_cls = self._make_sync(model=ModelWithFKChild)
        topic = sync_cls().sink.make_topic()
        relation_models = [t.model for t in topic.relation_transforms]
        self.assertIn(RelatedModel, relation_models)
        self.assertNotIn(ModelWithFK, relation_models)

    def _nullable_fk_transform(self):
        return RelationTransform(
            source="nullable_related_id",
            target="nullable_related",
            model=RelatedModel,
            id_field="id",
        )

    def _make_nullable_fk_topic(self):
        sync_cls = self._make_sync(
            model=ModelWithNullableFK,
            consume_transforms=[self._nullable_fk_transform()],
        )
        return sync_cls().sink.make_topic()

    def _get_relations(self, topic, msg_value):
        with mock.patch.object(
            topic,
            "deserialize",
            side_effect=[{"id": 1}, msg_value],
        ):
            return list(topic.get_relations(mock.Mock()))

    def test_relations_are_named_from_the_transformed_value(self):
        sync_cls = self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                StaticValueTransform(source="related_id", value=42),
            ],
        )
        topic = sync_cls().sink.make_topic()

        relations = self._get_relations(topic, {"related_id": 7})

        self.assertEqual(relations[0].id_value, 42)

    def test_relation_declared_first_is_named_from_the_raw_value(self):
        custom = RelationTransform(
            source="related_id",
            target="related",
            model=RelatedModel,
            id_field="id",
        )
        sync_cls = self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                custom,
                StaticValueTransform(source="related_id", value=42),
            ],
        )
        topic = sync_cls().sink.make_topic()

        relations = self._get_relations(topic, {"related_id": 7})

        self.assertEqual(relations[0].id_value, 7)

    def test_auto_detected_relations_resolve_last(self):
        sync_cls = self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                StaticValueTransform(source="related_id", value=42),
            ],
        )
        topic = sync_cls().sink.make_topic()

        kinds = [type(step).__name__ for step in topic.transforms]

        self.assertEqual(kinds, ["StaticValueTransform", "RelationTransform"])

    def test_declared_relation_keeps_its_place(self):
        custom = RelationTransform(
            source="related_id",
            target="related",
            model=RelatedModel,
            id_field="id",
        )
        sync_cls = self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                custom,
                StaticValueTransform(source="name", value="b"),
            ],
        )
        topic = sync_cls().sink.make_topic()

        kinds = [type(step).__name__ for step in topic.transforms]

        self.assertEqual(kinds, ["RelationTransform", "StaticValueTransform"])

    def test_lookup_rewrites_the_instance_lookup_field(self):
        sync_cls = self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                RelationTransform(
                    source="related_uuid",
                    target="related",
                    model=RelatedModel,
                    id_field="uuid",
                    lookup="other_related__uuid",
                ),
            ],
        )
        topic = sync_cls().sink.make_topic()

        result = topic.get_lookup_kwargs(ModelWithFK, {"related_uuid": "abc"}, {})

        self.assertEqual(result, {"other_related__uuid": "abc"})

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


@mock.patch(
    "django_kafka.conf.settings.MODEL_SYNC_CONSUMER",
    "django_kafka.consumer.Consumer",
)
class GetRelationsWalkTestCase(AbstractModelTestCase):
    """The walk resolves each relation so the ones depending on it can be yielded."""

    abstract_model = models.Model

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        class SyncModel(models.Model):  # noqa: DJ008
            first = models.ForeignKey(cls.model, models.CASCADE, related_name="+")
            second = models.ForeignKey(cls.model, models.CASCADE, related_name="+")

            class Meta:
                app_label = cls.__module__

        # only `_meta` is read - the walk never writes a row
        cls.sync_model = SyncModel

    def _topic(self, *transforms):
        sync_cls = make_sync(
            ModelSyncRegistry(),
            model=self.sync_model,
            source=None,
            sink=PythonAvroSink(),
            consume_transforms=list(transforms),
        )
        return sync_cls().sink.make_topic()

    def _dependent_chain(self):
        """Second relation's id exists only once the step before it has run."""
        return (
            RelationTransform(
                source="first_id",
                target="first",
                model=self.model,
                id_field="id",
            ),
            StaticValueTransform(source="second_id", value=5),
            RelationTransform(
                source="second_id",
                target="second",
                model=self.model,
                id_field="id",
            ),
        )

    def _get_relations(self, topic, msg_value):
        with mock.patch.object(
            topic,
            "deserialize",
            side_effect=[{"id": 1}, msg_value],
        ):
            return list(topic.get_relations(mock.Mock()))

    def test_names_the_next_relation_once_the_first_row_exists(self):
        existing = self.model.objects.create()
        topic = self._topic(*self._dependent_chain())

        relations = self._get_relations(
            topic,
            {"first_id": existing.id},
        )

        self.assertEqual([r.id_value for r in relations], [existing.id, 5])

    def test_missing_row_of_another_transform_is_not_swallowed(self):
        first, static, second = self._dependent_chain()
        topic = self._topic(first, static, second)
        existing = self.model.objects.create()

        with (
            mock.patch.object(static, "apply", side_effect=self.model.DoesNotExist),
            self.assertRaises(self.model.DoesNotExist),
        ):
            self._get_relations(topic, {"first_id": existing.id})

    def test_stops_at_the_first_missing_row(self):
        topic = self._topic(*self._dependent_chain())

        relations = self._get_relations(topic, {"first_id": 404})

        self.assertEqual([r.id_value for r in relations], [404])
