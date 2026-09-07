from unittest import TestCase, mock

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.models.model_sync import MessagePart
from django_kafka.models.model_sync.transforms import (
    LazySourceContentTypeMapping,
    LazyTargetContentTypeMapping,
    MappingTransform,
    RelationTransform,
)
from django_kafka.models.model_sync.transforms.field_transforms import _missing
from django_kafka.tests.test_models.test_model_sync.factories import (
    ModelWithFK,
    RelatedModel,
    SimpleModel,
)


class MappingTransformTestCase(TestCase):
    def _transform(self, mapping, value, *, default=_missing):
        mt = MappingTransform("foo", mapping=mapping, default_value=default)
        return mt.transform_value(mock.Mock(), {}, {"foo": value}, MessagePart.VALUE)

    def test_rejects_empty_mapping(self):
        with self.assertRaisesRegex(ValueError, "at least one"):
            MappingTransform("foo", mapping={})

    def test_rejects_varying_value_types(self):
        mt = MappingTransform("foo", mapping={1: 1, 2: "2"})

        with self.assertRaisesRegex(ValueError, "int | str"):
            mt.output_avro_type(mock.Mock(), mock.Mock())

    def test_rejects_foreign_default_value_type(self):
        mt = MappingTransform("foo", mapping={1: 1}, default_value=0.0)

        with self.assertRaisesRegex(ValueError, "int | float"):
            mt.output_avro_type(mock.Mock(), mock.Mock())

    def test_allows_nulls_if_other_value_types_match(self):
        mt = MappingTransform("foo", mapping={1: 1, 2: 2, 3: None}, default_value=0)

        try:
            output_type = mt.output_avro_type(mock.Mock(), mock.Mock())
        except ValueError:
            self.fail("Must not raise!")

        self.assertEqual(output_type, ["null", "int"])

    def test_transforms_existing_key(self):
        result = self._transform({"key": "value"}, "key")

        self.assertEqual(result, "value")

    def test_returns_default_if_given_and_key_missing(self):
        result = self._transform({"": ""}, "key", default="value")

        self.assertEqual(result, "value")

    def test_fails_if_no_default_given_and_key_missing(self):
        with self.assertRaisesRegex(DjangoKafkaError, "test"):
            self._transform({"": ""}, "test")


@mock.patch(
    "django.contrib.contenttypes.models.ContentType.objects.get_for_models",
    return_value={SimpleModel: mock.Mock(id=1), RelatedModel: mock.Mock(id=2)},
)
class LazyTargetContentTypeMappingTestCase(TestCase):
    def test_source_does_not_access_db_before_use(self, mock_get_for_models):
        LazySourceContentTypeMapping({SimpleModel: 5})

        mock_get_for_models.assert_not_called()

    def test_source_resolves_when_keys_accessed(self, mock_get_for_models):
        list(LazySourceContentTypeMapping({SimpleModel: 5}))

        mock_get_for_models.assert_called_once_with(SimpleModel)

    def test_source_translates_models_to_content_type_ids(self, _mock):
        mapping = LazySourceContentTypeMapping({RelatedModel: 5})

        self.assertEqual(dict(mapping), {2: 5})

    def test_source_fails_on_missing_models(self, _mock):
        with self.assertRaisesRegex(KeyError, "ModelWithFK"):
            dict(LazySourceContentTypeMapping({ModelWithFK: 5}))

    def test_target_does_not_access_db_before_use(self, mock_get_for_models):
        list(LazyTargetContentTypeMapping({5: SimpleModel}))

        mock_get_for_models.assert_not_called()

    def test_target_resolves_when_values_accessed(self, mock_get_for_models):
        dict(LazyTargetContentTypeMapping({5: SimpleModel}))

        mock_get_for_models.assert_called_once_with(SimpleModel)

    def test_target_translates_models_to_content_type_ids(self, _mock):
        mapping = LazyTargetContentTypeMapping({5: RelatedModel})

        self.assertEqual(dict(mapping), {5: 2})

    def test_target_fails_on_missing_models(self, _mock):
        with self.assertRaisesRegex(KeyError, "ModelWithFK"):
            dict(LazyTargetContentTypeMapping({5: ModelWithFK}))


class RelationTransformTestCase(TestCase):
    def _transform(self):
        return RelationTransform(
            source="related_id",
            target="related",
            model=mock.Mock(),
            id_field="id",
        )

    def test_resolves_instance_for_value(self):
        transform = self._transform()
        instance = transform.model.objects.get.return_value

        new_value = transform.apply(None, {}, {"related_id": 5, "name": "n"})[1]

        transform.model.objects.get.assert_called_once_with(id=5)
        self.assertEqual(new_value, {"related": instance, "name": "n"})

    def test_null_value_assigns_none_without_lookup(self):
        transform = self._transform()

        new_value = transform.apply(None, {}, {"related_id": None})[1]

        transform.model.objects.get.assert_not_called()
        self.assertEqual(new_value, {"related": None})

    def test_absent_value_assigns_none_without_lookup(self):
        transform = self._transform()

        new_value = transform.apply(None, {}, {})[1]

        transform.model.objects.get.assert_not_called()
        self.assertEqual(new_value, {"related": None})
