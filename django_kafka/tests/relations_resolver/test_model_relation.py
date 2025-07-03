from django.test import TestCase

from django_kafka.relations_resolver.relation import ModelRelation
from example.models import Order


# ruff: noqa: DJ008
class ModelRelationTestCase(TestCase):
    def test__init__(self):
        relation = ModelRelation(Order, id_field="id", id_value=1)

        self.assertEqual(relation.model, Order)
        self.assertEqual(relation.id_field, "id")
        self.assertEqual(relation.id_value, 1)
        self.assertEqual(
            relation.model_key,
            ModelRelation.get_model_key(Order),
        )

    def test_identifier(self):
        relation = ModelRelation(Order, id_field="id", id_value=1)

        self.assertEqual(
            relation.identifier(),
            f"{relation.model_key}-{relation.id_value}".lower(),
        )

    async def test_aexists(self):
        relation = ModelRelation(Order, id_field="id", id_value=1)
        self.assertFalse(await relation.aexists())

        await Order.objects.acreate(id=1)
        self.assertTrue(await relation.aexists())

    def test_serialize(self):
        relation = ModelRelation(Order, id_field="id", id_value=1)
        self.assertDictEqual(
            relation.serialize(),
            {
                "type": "MODEL",
                "kwargs": {
                    "model": ModelRelation.get_model_key(Order),
                    "id_value": 1,
                    "id_field": "id",
                },
            },
        )

    def test_deserialize(self):
        serialized_relation = {
            "type": "MODEL",
            "kwargs": {
                "model": ModelRelation.get_model_key(Order),
                "id_value": 1,
                "id_field": "id",
            },
        }
        relation = ModelRelation.deserialize(**serialized_relation["kwargs"])

        self.assertEqual(relation.model, Order)
        self.assertEqual(relation.id_field, serialized_relation["kwargs"]["id_field"])
        self.assertEqual(relation.id_value, serialized_relation["kwargs"]["id_value"])
        self.assertEqual(relation.model_key, serialized_relation["kwargs"]["model"])
