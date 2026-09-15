from unittest import TestCase, mock

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.models.model_sync.transforms import (
    RelationTransform,
    RemoteContentTypes,
    RemoteContentTypeTransform,
)

APPS = "django_kafka.models.model_sync.transforms.apps"
CONTENT_TYPE = "django.contrib.contenttypes.models.ContentType"


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


class RemoteContentTypesTestCase(TestCase):
    def setUp(self):
        self.model = mock.Mock()
        self.local = mock.Mock(id=42)
        self.content_types = RemoteContentTypes(models={7: self.model})

    @mock.patch(CONTENT_TYPE)
    def test_local_id_for_producer_content_type_id(self, content_type):
        content_type.objects.get_for_models.return_value = {self.model: self.local}

        self.assertEqual(self.content_types.id_for(7), 42)
        content_type.objects.get_for_models.assert_called_once_with(self.model)

    @mock.patch(CONTENT_TYPE)
    def test_unmapped_content_type_id(self, content_type):
        content_type.objects.get_for_models.return_value = {self.model: self.local}

        with self.assertRaises(DjangoKafkaError):
            self.content_types.id_for(12)

    @mock.patch(CONTENT_TYPE)
    @mock.patch(APPS)
    def test_model_path_is_resolved_on_use(self, apps, content_type):
        content_types = RemoteContentTypes(models={7: "sales.Deal"})
        content_type.objects.get_for_models.return_value = {
            apps.get_model.return_value: self.local,
        }

        self.assertEqual(content_types.id_for(7), 42)
        apps.get_model.assert_called_once_with("sales.Deal")

    @mock.patch(CONTENT_TYPE)
    def test_content_types_are_resolved_once(self, content_type):
        content_type.objects.get_for_models.return_value = {self.model: self.local}

        self.content_types.id_for(7)
        self.content_types.id_for(7)

        content_type.objects.get_for_models.assert_called_once_with(self.model)


class RemoteContentTypeTransformTestCase(TestCase):
    def setUp(self):
        self.model = mock.Mock()
        self.transform = RemoteContentTypeTransform(
            content_types=RemoteContentTypes(models={7: self.model}),
        )

    @mock.patch(CONTENT_TYPE)
    def test_replaces_producer_id_with_local_id(self, content_type):
        content_type.objects.get_for_models.return_value = {
            self.model: mock.Mock(id=42),
        }

        new_value = self.transform.apply(
            None,
            {},
            {"content_type_id": 7, "object_id": 3312},
        )[1]

        self.assertEqual(new_value, {"content_type_id": 42, "object_id": 3312})

    @mock.patch(CONTENT_TYPE)
    def test_null_value_needs_no_lookup(self, content_type):
        new_value = self.transform.apply(None, {}, {"content_type_id": None})[1]

        content_type.objects.get_for_models.assert_not_called()
        self.assertEqual(new_value, {"content_type_id": None})

    @mock.patch(CONTENT_TYPE)
    def test_absent_value_needs_no_lookup(self, content_type):
        new_value = self.transform.apply(None, {}, {"object_id": 3312})[1]

        content_type.objects.get_for_models.assert_not_called()
        self.assertEqual(new_value, {"content_type_id": None, "object_id": 3312})
