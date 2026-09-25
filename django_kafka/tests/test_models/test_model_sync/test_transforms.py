from unittest import TestCase, mock

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.models import WaitingMessage
from django_kafka.models.model_sync.transforms import (
    MappingTransform,
    RelationTransform,
    RemoteContentTypes,
)

GET_FOR_MODELS = "django.contrib.contenttypes.models.ContentType.objects.get_for_models"


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


class MappingTransformTestCase(TestCase):
    def _transform(self, mapping=None):
        return MappingTransform(source="status", mapping=mapping or {1: "open"})

    def test_replaces_value_with_its_mapping(self):
        new_value = self._transform().apply(None, {}, {"status": 1, "text": "x"})[1]

        self.assertEqual(new_value, {"status": "open", "text": "x"})

    def test_null_value_needs_no_lookup(self):
        new_value = self._transform().apply(None, {}, {"status": None})[1]

        self.assertEqual(new_value, {"status": None})

    def test_absent_value_needs_no_lookup(self):
        new_value = self._transform().apply(None, {}, {"text": "x"})[1]

        self.assertEqual(new_value, {"status": None, "text": "x"})

    def test_unmapped_value(self):
        with self.assertRaises(DjangoKafkaError) as error:
            self._transform().apply(None, {}, {"status": 9})

        self.assertIn("status=9", str(error.exception))

    def test_default_replaces_an_unmapped_value(self):
        transform = MappingTransform(source="status", mapping={1: "open"}, default=None)

        new_value = transform.apply(None, {}, {"status": 9})[1]

        self.assertEqual(new_value, {"status": None})

    def test_default_widens_the_avro_type(self):
        transform = MappingTransform(source="status", mapping={1: "open"}, default=None)

        self.assertEqual(
            transform.output_avro_type(None, None),
            ["null", "string"],
        )

    def test_avro_type_comes_from_the_mapped_values(self):
        self.assertEqual(self._transform().output_avro_type(None, None), "string")

    def test_avro_type_is_nullable_when_a_value_is_none(self):
        transform = self._transform(mapping={1: "open", 2: None})

        self.assertEqual(transform.output_avro_type(None, None), ["null", "string"])

    def test_avro_type_needs_one_value_type(self):
        transform = self._transform(mapping={1: "open", 2: 2})

        with self.assertRaisesRegex(ValueError, r"got \['int', 'str'\]"):
            transform.output_avro_type(None, None)


class RemoteContentTypesTestCase(TestCase):
    def setUp(self):
        self.model = mock.Mock()
        self.local = mock.Mock(id=42)
        self.content_types = RemoteContentTypes(models={7: self.model})

    @mock.patch(GET_FOR_MODELS)
    def test_remote_id_resolves_to_local_id(self, get_for_models):
        get_for_models.return_value = {self.model: self.local}

        self.assertEqual(self.content_types[7], 42)
        get_for_models.assert_called_once_with(self.model)

    @mock.patch(GET_FOR_MODELS)
    def test_unmapped_remote_id(self, get_for_models):
        get_for_models.return_value = {self.model: self.local}

        with self.assertRaises(KeyError):
            self.content_types[12]

    @mock.patch(GET_FOR_MODELS)
    def test_model_path_is_resolved_on_use(self, get_for_models):
        content_types = RemoteContentTypes(models={7: "django_kafka.WaitingMessage"})
        get_for_models.return_value = {WaitingMessage: self.local}

        self.assertEqual(content_types[7], 42)
        get_for_models.assert_called_once_with(WaitingMessage)

    @mock.patch(GET_FOR_MODELS)
    def test_content_types_are_resolved_once(self, get_for_models):
        get_for_models.return_value = {self.model: self.local}

        self.content_types[7]
        self.content_types[7]

        get_for_models.assert_called_once_with(self.model)

    @mock.patch(GET_FOR_MODELS)
    def test_inverse_maps_local_id_back_to_remote(self, get_for_models):
        get_for_models.return_value = {self.model: self.local}

        self.assertEqual(self.content_types.inverse[42], 7)

    @mock.patch(GET_FOR_MODELS)
    def test_inverse_resolves_no_earlier_than_the_declaration(self, get_for_models):
        self.content_types.inverse  # noqa: B018

        get_for_models.assert_not_called()

    @mock.patch(GET_FOR_MODELS)
    def test_inverse_rejects_two_remote_ids_on_one_model(self, get_for_models):
        get_for_models.return_value = {self.model: self.local}
        content_types = RemoteContentTypes(models={7: self.model, 8: self.model})

        with self.assertRaisesRegex(ValueError, "same"):
            content_types.inverse[42]

    @mock.patch(GET_FOR_MODELS)
    def test_remote_ids_are_known_without_the_database(self, get_for_models):
        self.assertEqual(list(self.content_types), [7])
        self.assertEqual(len(self.content_types), 1)

        get_for_models.assert_not_called()


class RemoteContentTypeMappingTestCase(TestCase):
    """The pair as documented: a mapping transform over the lazy content types."""

    @mock.patch(GET_FOR_MODELS)
    def test_maps_remote_content_type_id_to_the_local_one(self, get_for_models):
        model = mock.Mock()
        get_for_models.return_value = {model: mock.Mock(id=42)}
        transform = MappingTransform(
            source="content_type_id",
            mapping=RemoteContentTypes(models={7: model}),
        )

        new_value = transform.apply(
            None,
            {},
            {"content_type_id": 7, "object_id": 3312},
        )[1]

        self.assertEqual(new_value, {"content_type_id": 42, "object_id": 3312})
