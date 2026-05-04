import json
from typing import TypedDict
from unittest import TestCase, mock

from django_kafka.models.model_sync import (
    EnricherTransform,
    SyncMethodTransform,
)
from django_kafka.models.model_sync.enricher import ModelSyncEnricher
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.schema.avro import AvroSchema
from django_kafka.topic.reproducer import ReproduceTopic

from .factories import SimpleModel, make_sync


class _ExtraFields(TypedDict):
    brand_name: str


def _make_sync_with_enrich(registry, **attrs):
    def enrich(key, value) -> _ExtraFields:
        return {"brand_name": "brand"}

    attrs["enrich"] = staticmethod(enrich)
    attrs.setdefault("enrich_transforms", [EnricherTransform()])
    return make_sync(registry, **attrs)


class ModelSyncEnricherForSyncTestCase(TestCase):
    def test_returns_reproduce_topic(self):
        registry = ModelSyncRegistry()
        sync_cls = _make_sync_with_enrich(registry)
        topic = ModelSyncEnricher.for_sync(sync_cls)
        self.assertIsInstance(topic, ReproduceTopic)

    def test_reproduce_topic_name_is_source_topic(self):
        registry = ModelSyncRegistry()
        sync_cls = _make_sync_with_enrich(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", "app"):
            topic = ModelSyncEnricher.for_sync(sync_cls)
            self.assertEqual(topic.name, sync_cls.source_topic())

    def test_enricher_name_is_enriched_topic(self):
        registry = ModelSyncRegistry()
        sync_cls = _make_sync_with_enrich(registry)
        with mock.patch("django_kafka.conf.settings.MODEL_SYNC_TOPIC_PREFIX", "app"):
            topic = ModelSyncEnricher.for_sync(sync_cls)
            self.assertEqual(topic.reproducer.name, sync_cls.get_enriched_topic())


class ModelSyncEnricherSchemaTestCase(TestCase):
    def test_extended_schemas_adds_enrich_fields_to_value(self):
        registry = ModelSyncRegistry()
        sync_cls = _make_sync_with_enrich(registry)
        enricher = ModelSyncEnricher(sync_cls)

        source_value = AvroSchema.from_model(SimpleModel, name="Value").to_json()
        extended_value = enricher._extended_schemas(None, source_value)[1]

        field_names = [f["name"] for f in json.loads(extended_value)["fields"]]
        self.assertIn("brand_name", field_names)
        self.assertIn("name", field_names)

    def test_extended_schemas_passthrough_without_transforms(self):
        registry = ModelSyncRegistry()
        sync_cls = make_sync(registry)
        enricher = ModelSyncEnricher(sync_cls)

        source_value = '{"type":"record","name":"Value","fields":[]}'
        extended = enricher._extended_schemas(None, source_value)[1]
        self.assertEqual(extended, source_value)


class ModelSyncEnricherReproduceTestCase(TestCase):
    def _make_enricher(self, enrich_fn=None, transforms=None):
        registry = ModelSyncRegistry()

        def enrich(key, value) -> _ExtraFields:
            return {"brand_name": "brand"}

        attrs = {
            "enrich": staticmethod(enrich_fn or enrich),
            "enrich_transforms": transforms or [EnricherTransform()],
        }
        sync_cls = make_sync(registry, **attrs)
        return ModelSyncEnricher(sync_cls)

    def test_upsert_merges_value_and_enrich_result(self):
        def enrich(key, value) -> _ExtraFields:
            return {"brand_name": "enriched"}

        enricher = self._make_enricher(enrich_fn=enrich)
        with mock.patch.object(enricher, "produce") as mock_produce:
            enricher.reproduce({"id": 1}, {"name": "test"}, is_deletion=False)
        mock_produce.assert_called_once_with(
            key={"id": 1},
            value={"name": "test", "brand_name": "enriched"},
            key_serializer_kwargs={},
            value_serializer_kwargs={},
        )

    def test_deletion_produces_tombstone(self):
        enricher = self._make_enricher()
        with mock.patch.object(enricher, "produce") as mock_produce:
            enricher.reproduce({"id": 1}, None, is_deletion=True)
        mock_produce.assert_called_once_with(
            key={"id": 1},
            value=None,
            key_serializer_kwargs={},
        )

    def test_schemas_passed_to_produce(self):
        enricher = self._make_enricher()
        with mock.patch.object(enricher, "produce") as mock_produce:
            enricher.reproduce(
                {"id": 1}, {"name": "test"}, is_deletion=False,
                key_schema='{"type":"record","name":"Key","fields":[]}',
                value_schema='{"type":"record","name":"Value","fields":[]}',
            )
        call_kwargs = mock_produce.call_args.kwargs
        # both schemas are rebuilt by walking the transforms; assert the
        # essentials rather than a byte-equal match
        key_schema = json.loads(call_kwargs["key_serializer_kwargs"]["schema_str"])
        value_schema = json.loads(call_kwargs["value_serializer_kwargs"]["schema_str"])
        self.assertEqual(key_schema["name"], "Key")
        self.assertEqual(value_schema["type"], "record")
