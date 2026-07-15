from unittest import TestCase, mock

from django_kafka.models.model_sync.transforms import RelationTransform


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
