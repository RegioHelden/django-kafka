from django.test import SimpleTestCase

from django_kafka.relations_resolver.relation import RelationType


class RelationModel:
    pass


class RelationTypeTestCase(SimpleTestCase):
    def test_type_model_instance(self):
        serialized_relation = {
            "type": "MODEL",
            "kwargs": {
                "model": f"{RelationModel.__module__}.{RelationModel.__name__}",
                "id_value": 1,
                "id_field": "id",
            },
        }

        relation = RelationType.instance(serialized_relation)

        self.assertEqual(relation.model, RelationModel)
        self.assertEqual(relation.id_field, serialized_relation["kwargs"]["id_field"])
        self.assertEqual(relation.id_value, serialized_relation["kwargs"]["id_value"])
        self.assertEqual(relation.model_path, serialized_relation["kwargs"]["model"])
