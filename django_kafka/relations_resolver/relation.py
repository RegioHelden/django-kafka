from abc import ABC, abstractmethod

from django.db.models import Model


class ModelRelation(ABC):
    model: type[Model]
    object_id: int
    id_field: str
    model_path: str

    def __init__(self, model: type[Model], object_id: int | str, id_field: str):
        self.model = model
        self.object_id = object_id
        self.id_field = id_field
        self.model_path = f"{model.__module__}.{self.model.__name__}"

    async def get(self) -> Model:
        return await self.model.objects.aget(**{self.id_field: self.object_id})

    async def exists(self) -> bool:
        return await self.model.objects.filter(**{self.id_field: self.object_id}).aexists()

    @abstractmethod
    async def has_waiting_messages(self) -> bool:
        """ identifies weather some messages are already waiting for the dependency to arrive"""
