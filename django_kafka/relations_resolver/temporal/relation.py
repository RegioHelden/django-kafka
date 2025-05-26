from pydoc import locate

from django_kafka.relations_resolver.relation import ModelRelation


class TemporalModelRelation(ModelRelation):
    def serialize(self) -> dict:
        return {
            "model": self.model_path,
            "object_id": self.object_id,
            "id_field": self.id_field,
        }

    @classmethod
    def deserialize(
        cls,
        model: str,
        object_id: int | str,
        id_field: str,
    ) -> "TemporalModelRelation":
        return cls(locate(model), object_id, id_field)
