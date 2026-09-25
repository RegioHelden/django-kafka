from unittest import TestCase, mock

from django_kafka.models.model_sync import (
    EnricherTransform,
    MessagePart,
    RelationTransform,
    StaticValueTransform,
)
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource

from .factories import BidirectionalModel, ModelWithFK, SimpleModel, make_sync


class HasEnrichTestCase(TestCase):
    def test_no_transforms_not_detected(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        self.assertFalse(sync_cls.has_enrich())

    def test_enrich_transforms_detected(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            enrich_transforms=[EnricherTransform()],
        )
        self.assertTrue(sync_cls.has_enrich())


class TopicNamesTestCase(TestCase):
    def test_source_topic_with_prefix(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", "myapp"):
            self.assertEqual(
                sync_cls.source_topic(),
                f"myapp.public.{SimpleModel._meta.db_table}",
            )

    def test_source_topic_without_prefix(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", None):
            self.assertEqual(
                sync_cls.source_topic(),
                f"public.{SimpleModel._meta.db_table}",
            )

    def test_enriched_topic_falls_back_to_raw_with_prefix(self):
        # No `topic` set — public name defaults to the raw debezium topic.
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", "myapp"):
            self.assertEqual(
                sync_cls.get_enriched_topic(),
                f"myapp.public.{SimpleModel._meta.db_table}",
            )

    def test_enriched_topic_falls_back_to_raw_without_prefix(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", None):
            self.assertEqual(
                sync_cls.get_enriched_topic(),
                f"public.{SimpleModel._meta.db_table}",
            )

    def test_topic_overrides_source_topic(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry, topic="myapp.user")
        self.assertEqual(sync_cls.source_topic(), "myapp.user")

    def test_topic_is_enriched_topic_when_no_enricher(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry, topic="myapp.user")
        self.assertEqual(sync_cls.get_enriched_topic(), "myapp.user")


class ValidateBidirectionalTestCase(TestCase):
    def test_bidirectional_requires_kafka_connect_skip_model(self):
        registry = ModelSyncRegistry()
        with self.assertRaises(ValueError) as ctx:
            make_sync(
                registry,
                model=SimpleModel,
                source=DbzPostgresSource(),
                sink=DbzJdbcSink(),
            )
        self.assertIn("KafkaConnectSkipModel", str(ctx.exception))

    def test_bidirectional_requires_topic(self):
        registry = ModelSyncRegistry()
        with self.assertRaises(ValueError) as ctx:
            make_sync(
                registry,
                model=BidirectionalModel,
                source=DbzPostgresSource(),
                sink=DbzJdbcSink(),
            )
        self.assertIn("topic", str(ctx.exception))

    def test_bidirectional_with_topic_passes(self):
        registry = ModelSyncRegistry()
        make_sync(
            registry,
            model=BidirectionalModel,
            topic="myapp.user",
            source=DbzPostgresSource(),
            sink=DbzJdbcSink(),
        )

    def test_source_only_does_not_require_kafka_connect_skip_model(self):
        registry = ModelSyncRegistry()
        make_sync(registry, model=SimpleModel, source=DbzPostgresSource())

    def test_sink_only_does_not_require_kafka_connect_skip_model(self):
        registry = ModelSyncRegistry()
        make_sync(registry, model=SimpleModel, source=None, sink=DbzJdbcSink())


class ConsumeTransformValidationTestCase(TestCase):
    def _make_sync(self, **attrs):
        return make_sync(ModelSyncRegistry(), source=None, sink=DbzJdbcSink(), **attrs)

    def test_rejects_key_side_consume_transform(self):
        with self.assertRaisesRegex(ValueError, "apply_to"):
            self._make_sync(
                consume_transforms=[
                    StaticValueTransform(
                        source="status",
                        value=2,
                        apply_to=MessagePart.BOTH,
                    ),
                ],
            )

    def test_rejects_relation_transform_for_an_unknown_fk(self):
        with self.assertRaisesRegex(ValueError, "not a foreign key"):
            self._make_sync(
                consume_transforms=[
                    RelationTransform(source="related_uuid", target="related"),
                ],
            )

    def test_rejects_wait_only_relation_transform_on_a_renamed_field(self):
        with self.assertRaisesRegex(ValueError, "not a foreign key"):
            self._make_sync(
                model=ModelWithFK,
                consume_transforms=[RelationTransform(source="related_uuid")],
            )

    def test_accepts_a_relation_transform_naming_a_real_fk(self):
        self._make_sync(
            model=ModelWithFK,
            consume_transforms=[
                RelationTransform(source="related_uuid", target="related"),
                RelationTransform(source="related_id"),
            ],
        )

    def test_rejects_relation_transform_on_the_enrich_side(self):
        with self.assertRaisesRegex(ValueError, "enrich_transforms"):
            self._make_sync(
                enrich_transforms=[
                    RelationTransform(source="related_id", target="related"),
                ],
            )
