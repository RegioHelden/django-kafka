from unittest import TestCase, mock

from django_kafka.models.model_sync import ModelSync, EnricherTransform
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource

from .factories import BidirectionalModel, SimpleModel, make_sync


class HasEnrichTestCase(TestCase):
    def test_no_transforms_not_detected(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        self.assertFalse(sync_cls.has_enrich())

    def test_enrich_transforms_detected(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry, enrich_transforms=[EnricherTransform()],
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
            self.assertEqual(sync_cls.source_topic(), f"public.{SimpleModel._meta.db_table}")

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
        sync_cls = make_sync(registry, topic="ca.user")
        self.assertEqual(sync_cls.source_topic(), "ca.user")

    def test_topic_is_enriched_topic_when_no_enricher(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry, topic="ca.user")
        self.assertEqual(sync_cls.get_enriched_topic(), "ca.user")


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
            topic="ca.user",
            source=DbzPostgresSource(),
            sink=DbzJdbcSink(),
        )

    def test_source_only_does_not_require_kafka_connect_skip_model(self):
        registry = ModelSyncRegistry()
        make_sync(registry, model=SimpleModel, source=DbzPostgresSource())

    def test_sink_only_does_not_require_kafka_connect_skip_model(self):
        registry = ModelSyncRegistry()
        make_sync(registry, model=SimpleModel, source=None, sink=DbzJdbcSink())


