from unittest import TestCase, mock

from django_kafka.models.model_sync.fields import ExcludeFields, IncludeFields
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource

from .factories import BidirectionalModel, SimpleModel, make_sync

_SINK_DB = {
    "HOST": "localhost",
    "PORT": 5432,
    "NAME": "testdb",
    "USER": "user",
    "PASSWORD": "pass",
}

class DbzJdbcSinkConfigTestCase(TestCase):
    def _get_config(self, registry=None, model=None, **make_attrs):
        registry = registry or ModelSyncRegistry()
        make_attrs.setdefault("source", None)
        make_attrs.setdefault("sink", DbzJdbcSink(topics=["test.topic"]))
        with mock.patch("django_kafka.conf.settings.CONNECT_DATABASE", _SINK_DB):
            with mock.patch("django_kafka.conf.settings.SCHEMA_REGISTRY", {"url": "http://sr"}):
                sync_cls = make_sync(registry, model=model or SimpleModel, **make_attrs)
                return sync_cls().sink.config

    def test_basic_config_keys(self):
        cfg = self._get_config()
        self.assertEqual(cfg["connector.class"], "io.debezium.connector.jdbc.JdbcSinkConnector")
        self.assertIn("connection.url", cfg)
        self.assertIn("topics", cfg)

    def test_no_kafka_skip_transform_for_unidirectional(self):
        cfg = self._get_config()
        transforms = cfg.get("transforms", "")
        self.assertNotIn("SetKafkaSkip", transforms)

    def test_kafka_skip_transform_added_for_bidirectional(self):
        cfg = self._get_config(
            model=BidirectionalModel,
            topic="ca.user",
            source=DbzPostgresSource(),
            sink=DbzJdbcSink(topics=["test.topic"]),
        )
        self.assertIn("MsRmKafkaSkip", cfg["transforms"])
        self.assertIn("MsAddKafkaSkip", cfg["transforms"])
        self.assertIn("MsCastKafkaSkip", cfg["transforms"])
        self.assertEqual(
            cfg["transforms.MsRmKafkaSkip.type"],
            "org.apache.kafka.connect.transforms.ReplaceField$Value",
        )
        self.assertEqual(cfg["transforms.MsRmKafkaSkip.exclude"], "kafka_skip")
        self.assertEqual(
            cfg["transforms.MsAddKafkaSkip.type"],
            "org.apache.kafka.connect.transforms.InsertField$Value",
        )
        self.assertEqual(cfg["transforms.MsAddKafkaSkip.static.field"], "kafka_skip")
        self.assertIs(cfg["transforms.MsAddKafkaSkip.static.value"], True)
        self.assertEqual(
            cfg["transforms.MsCastKafkaSkip.type"],
            "org.apache.kafka.connect.transforms.Cast$Value",
        )
        self.assertEqual(cfg["transforms.MsCastKafkaSkip.spec"], "kafka_skip:boolean")

    def test_sink_fields_overrides_model_sync_fields(self):
        cfg = self._get_config(
            sink=DbzJdbcSink(topics=["test.topic"], fields=ExcludeFields("name")),
            fields=IncludeFields("name"),
            source=None,
        )
        self.assertIn("MsExcludeFields", cfg["transforms"])
        self.assertNotIn("MsIncludeFields", cfg["transforms"])

    def test_kafka_skip_combined_with_fields_transform(self):
        cfg = self._get_config(
            model=BidirectionalModel,
            topic="ca.user",
            source=DbzPostgresSource(),
            sink=DbzJdbcSink(topics=["test.topic"]),
            fields=IncludeFields("name"),
        )
        transforms = cfg["transforms"].split(",")
        self.assertIn("MsIncludeFields", transforms)
        self.assertIn("MsRmKafkaSkip", transforms)
        self.assertIn("MsAddKafkaSkip", transforms)
        self.assertIn("MsCastKafkaSkip", transforms)
