from unittest import TestCase, mock

from django_kafka.models.model_sync.fields import (
    ExcludeFields,
    IncludeFields,
)
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import (
    DbzPostgresSource,
)

from .factories import BidirectionalModel, SimpleModel, make_sync


class DbzPostgresSourceSetupTestCase(TestCase):
    def _setup_config(self, model=None, topic_prefix="app", **attrs):
        registry = ModelSyncRegistry()
        attrs.setdefault("source", DbzPostgresSource())
        sync_cls = make_sync(registry, model=model or SimpleModel, **attrs)
        sync = sync_cls()
        config = {"topic.prefix": topic_prefix, "transforms": ""}
        sync.source.setup(config)
        return config

    def test_set_table(self):
        config = self._setup_config()
        tables = config["table.include.list"].split(",")
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}",
            tables,
        )

    def test_set_replica_identity(self):
        config = self._setup_config()
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}:FULL",
            config["replica.identity.autoset.values"],
        )

    def test_set_msg_key_fields(self):
        config = self._setup_config(
            source=DbzPostgresSource(msg_key_fields=["kafka_uuid"]),
        )
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}:kafka_uuid",
            config["message.key.columns"],
        )

    def test_set_fields_include(self):
        config = self._setup_config(fields=IncludeFields(["name"]))
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}.name",
            config["column.include.list"],
        )

    def test_set_fields_exclude(self):
        config = self._setup_config(fields=ExcludeFields(["name"]))
        include_list = config["column.include.list"]
        self.assertNotIn(
            f"public.{SimpleModel._meta.db_table}.name",
            include_list,
        )
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}.id",
            include_list,
        )

    def test_set_fields_all_when_none(self):
        config = self._setup_config(fields=None)
        self.assertIn(
            f"public.{SimpleModel._meta.db_table}.*",
            config["column.include.list"],
        )


class DbzPostgresSourceKafkaSkipFilterTestCase(TestCase):
    def _setup_bidirectional(self, topic_prefix="app"):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            model=BidirectionalModel,
            topic="ca.user",
            source=DbzPostgresSource(msg_key_fields=["kafka_uuid"]),
            sink=DbzJdbcSink(pk_fields=["kafka_uuid"]),
        )
        sync = sync_cls()
        config = {"topic.prefix": topic_prefix, "transforms": ""}
        sync.source.setup(config)
        return config, sync_cls

    def test_kafka_skip_filter_added_for_bidirectional(self):
        config, sync_cls = self._setup_bidirectional()
        transform_name = f"MsKafkaSkipFilter{sync_cls.__name__}"
        transforms = config["transforms"].split(",")
        self.assertIn(transform_name, transforms)

    def test_kafka_skip_filter_prepended(self):
        config, sync_cls = self._setup_bidirectional()
        transform_name = f"MsKafkaSkipFilter{sync_cls.__name__}"
        transforms = config["transforms"].split(",")
        self.assertEqual(transforms[0], transform_name)

    def test_kafka_skip_filter_config(self):
        config, sync_cls = self._setup_bidirectional()
        name = f"MsKafkaSkipFilter{sync_cls.__name__}"
        self.assertEqual(
            config[f"transforms.{name}.type"],
            "io.debezium.transforms.Filter",
        )
        self.assertEqual(
            config[f"transforms.{name}.language"],
            "jsr223.groovy",
        )
        db_table = f"public.{BidirectionalModel._meta.db_table}"
        self.assertEqual(
            config[f"transforms.{name}.topic.regex"],
            f"app.{db_table}",
        )

    def test_no_kafka_skip_filter_for_unidirectional(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            source=DbzPostgresSource(),
        )
        sync = sync_cls()
        config = {"topic.prefix": "app", "transforms": ""}
        sync.source.setup(config)
        self.assertNotIn("MsKafkaSkipFilter", config.get("transforms", ""))


class DbzPostgresSourceRerouteTestCase(TestCase):
    def test_reroute_added_when_topic_set(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            topic="ca.user",
            source=DbzPostgresSource(),
        )
        sync = sync_cls()
        config = {"topic.prefix": "app", "transforms": ""}
        sync.source.setup(config)
        transform_name = f"MsReroute{sync_cls.__name__}"
        transforms = config["transforms"].split(",")
        self.assertIn(transform_name, transforms)

    def test_reroute_appended(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            topic="ca.user",
            source=DbzPostgresSource(),
        )
        sync = sync_cls()
        config = {"topic.prefix": "app", "transforms": ""}
        sync.source.setup(config)
        transform_name = f"MsReroute{sync_cls.__name__}"
        transforms = config["transforms"].split(",")
        self.assertEqual(transforms[-1], transform_name)

    def test_reroute_config(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            topic="ca.user",
            source=DbzPostgresSource(),
        )
        sync = sync_cls()
        config = {"topic.prefix": "app", "transforms": ""}
        sync.source.setup(config)
        name = f"MsReroute{sync_cls.__name__}"
        self.assertEqual(
            config[f"transforms.{name}.type"],
            "io.debezium.transforms.ByLogicalTableRouter",
        )
        self.assertEqual(
            config[f"transforms.{name}.topic.replacement"],
            "ca.user",
        )

    def test_no_reroute_without_topic(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(
            registry,
            source=DbzPostgresSource(),
        )
        sync = sync_cls()
        config = {"topic.prefix": "app", "transforms": ""}
        sync.source.setup(config)
        self.assertNotIn("MsReroute", config.get("transforms", ""))
