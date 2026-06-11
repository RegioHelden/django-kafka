from unittest import TestCase

from django_kafka.models.model_sync.fields import (
    ExcludeFields,
    IncludeFields,
)
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import (
    DbzPostgresSource,
)

from .factories import BidirectionalModel, SimpleModel, make_sync, make_sync_with_enrich


def _make_source_config(topic_prefix="app", **make_attrs):
    registry = ModelSyncRegistry()
    make_attrs.setdefault("source", DbzPostgresSource())
    sync_cls = make_sync(registry, **make_attrs)
    config = {"topic.prefix": topic_prefix, "transforms": ""}
    sync_cls().source.setup(config)
    return config, sync_cls


class DbzPostgresSourceSetupTestCase(TestCase):
    def test_set_table(self):
        config = _make_source_config()[0]
        tables = config["table.include.list"].split(",")
        self.assertIn(f"public.{SimpleModel._meta.db_table}", tables)

    def test_set_replica_identity(self):
        config = _make_source_config()[0]
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}:FULL",
            config["replica.identity.autoset.values"],
        )

    def test_set_msg_key_fields(self):
        config = _make_source_config(
            source=DbzPostgresSource(msg_key_fields=["kafka_uuid"]),
        )[0]
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}:kafka_uuid",
            config["message.key.columns"],
        )

    def test_set_fields_include(self):
        config = _make_source_config(fields=IncludeFields(["name"]))[0]
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}.name",
            config["column.include.list"],
        )

    def test_set_fields_exclude(self):
        config = _make_source_config(fields=ExcludeFields(["name"]))[0]
        include_list = config["column.include.list"]
        self.assertNotIn(f"public.{SimpleModel._meta.db_table}.name", include_list)
        self.assertIn(f"public.{SimpleModel._meta.db_table}.id", include_list)

    def test_set_fields_all_when_none(self):
        config = _make_source_config(fields=None)[0]
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}.*",
            config["column.include.list"],
        )


class DbzPostgresSourceKafkaSkipFilterTestCase(TestCase):
    def setUp(self):
        self.config, self.sync_cls = _make_source_config(
            model=BidirectionalModel,
            topic="myapp.user",
            source=DbzPostgresSource(msg_key_fields=["kafka_uuid"]),
            sink=DbzJdbcSink(pk_fields=["kafka_uuid"]),
        )

    def test_kafka_skip_filter_added_for_bidirectional(self):
        transform_name = f"MsKafkaSkipFilter{self.sync_cls.__name__}"
        self.assertIn(transform_name, self.config["transforms"].split(","))

    def test_kafka_skip_filter_prepended(self):
        transform_name = f"MsKafkaSkipFilter{self.sync_cls.__name__}"
        self.assertEqual(self.config["transforms"].split(",")[0], transform_name)

    def test_kafka_skip_filter_config(self):
        name = f"MsKafkaSkipFilter{self.sync_cls.__name__}"
        self.assertEqual(
            self.config[f"transforms.{name}.type"],
            "io.debezium.transforms.Filter",
        )
        self.assertEqual(self.config[f"transforms.{name}.language"], "jsr223.groovy")
        db_table = f"public.{BidirectionalModel._meta.db_table}"
        self.assertEqual(
            self.config[f"transforms.{name}.topic.regex"],
            f"app.{db_table}",
        )

    def test_no_kafka_skip_filter_for_unidirectional(self):
        config = _make_source_config()[0]
        self.assertNotIn("MsKafkaSkipFilter", config.get("transforms", ""))


class DbzPostgresSourceRerouteTestCase(TestCase):
    def setUp(self):
        self.config, self.sync_cls = _make_source_config(topic="myapp.user")

    def test_reroute_added_when_topic_set(self):
        transforms = self.config["transforms"].split(",")
        self.assertIn(f"MsReroute{self.sync_cls.__name__}", transforms)

    def test_reroute_appended(self):
        transforms = self.config["transforms"].split(",")
        self.assertEqual(transforms[-1], f"MsReroute{self.sync_cls.__name__}")

    def test_reroute_config(self):
        name = f"MsReroute{self.sync_cls.__name__}"
        self.assertEqual(
            self.config[f"transforms.{name}.type"],
            "io.debezium.transforms.ByLogicalTableRouter",
        )
        self.assertEqual(
            self.config[f"transforms.{name}.topic.replacement"],
            "myapp.user",
        )

    def test_no_reroute_without_topic(self):
        config = _make_source_config()[0]
        self.assertNotIn("MsReroute", config.get("transforms", ""))

    def test_no_reroute_when_enricher_present(self):
        # enricher publishes to `topic` itself; source must not also reroute
        registry = ModelSyncRegistry()
        sync_cls = make_sync_with_enrich(registry, topic="myapp.user")
        config = {"topic.prefix": "app", "transforms": ""}
        sync_cls().source.setup(config)
        self.assertNotIn("MsReroute", config.get("transforms", ""))
