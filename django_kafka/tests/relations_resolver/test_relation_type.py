from django.test import SimpleTestCase

from django_kafka.relations_resolver.relation import ModelRelation, RelationType
from example.models import Order


class RelationTypeTestCase(SimpleTestCase):
    def test_type_model_instance(self):
        serialized_relation = {
            "type": "MODEL",
            "kwargs": {
                "model": ModelRelation.get_model_key(Order),
                "id_value": 1,
                "id_field": "id",
            },
        }

        relation = RelationType.instance(serialized_relation)

        self.assertIsInstance(relation, ModelRelation)
        self.assertEqual(relation.model, Order)
        self.assertEqual(relation.id_field, serialized_relation["kwargs"]["id_field"])
        self.assertEqual(relation.id_value, serialized_relation["kwargs"]["id_value"])
        self.assertEqual(relation.model_key, serialized_relation["kwargs"]["model"])
