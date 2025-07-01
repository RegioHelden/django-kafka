from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.db.models import QuerySet

from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.tests.models import DynamicModelTestCase


# ruff: noqa: DJ008
class RelationModel(models.Model):
    objects = QuerySet.as_manager()


class ModelRelationTestCase(DynamicModelTestCase):
    model = RelationModel

    def test__init__(self):
        relation = ModelRelation(self.model, id_field="id", id_value=1)

        self.assertEqual(relation.model, self.model)
        self.assertEqual(relation.id_field, "id")
        self.assertEqual(relation.id_value, 1)
        self.assertEqual(
            relation.model_path,
            f"{self.model.__module__}.{self.model.__name__}",
        )

    def test_identifier(self):
        relation = ModelRelation(self.model, id_field="id", id_value=1)

        ct = ContentType.objects.get_for_model(self.model)
        self.assertEqual(
            relation.identifier(),
            f"ct{ct.id}-{ct.app_label}-{ct.model}-{relation.id_value}".lower(),
        )

    async def test_exists(self):
        relation = ModelRelation(self.model, id_field="id", id_value=1)
        self.assertFalse(await relation.exists())

        await RelationModel.objects.acreate(id=1)
        self.assertTrue(await relation.exists())

    def test_serialize(self):
        relation = ModelRelation(self.model, id_field="id", id_value=1)
        self.assertDictEqual(
            relation.serialize(),
            {
                "type": "MODEL",
                "kwargs": {
                    "model": f"{self.model.__module__}.{self.model.__name__}",
                    "id_value": 1,
                    "id_field": "id",
                },
            },
        )

    def test_deserialize(self):
        serialized_relation = {
            "type": "MODEL",
            "kwargs": {
                "model": f"{self.model.__module__}.{self.model.__name__}",
                "id_value": 1,
                "id_field": "id",
            },
        }
        relation = ModelRelation.deserialize(**serialized_relation["kwargs"])

        self.assertEqual(relation.model, self.model)
        self.assertEqual(relation.id_field, serialized_relation["kwargs"]["id_field"])
        self.assertEqual(relation.id_value, serialized_relation["kwargs"]["id_value"])
        self.assertEqual(relation.model_path, serialized_relation["kwargs"]["model"])
