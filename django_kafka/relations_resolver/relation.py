from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, TypedDict

from asgiref.sync import sync_to_async
from django.apps import apps
from temporalio import workflow

from django_kafka import kafka

if TYPE_CHECKING:
    from confluent_kafka import cimpl

with workflow.unsafe.imports_passed_through():
    from django.db.models import Model


class Relation(ABC):
    @abstractmethod
    def identifier(self) -> str: ...

    @abstractmethod
    async def aexists(self) -> bool: ...

    @abstractmethod
    def serialize(self) -> dict: ...

    @abstractmethod
    def deserialize(self, **kwargs) -> "Relation": ...

    async def amark_resolving(self):
        await kafka.relations_resolver.processor.amark_resolving(self)

    async def aidentifier(self) -> str:
        return await sync_to_async(self.identifier)()

    def type(self) -> str:
        return RelationType(self.__class__).name

    async def ahas_waiting_messages(self) -> bool:
        return await kafka.relations_resolver.processor.aexists(self)

    async def aadd_message(self, msg: "cimpl.Message"):
        await kafka.relations_resolver.processor.aadd_message(msg, self)

    async def aresolve(self):
        await kafka.relations_resolver.processor.aprocess_messages(self)


class ModelRelation(Relation):
    model: type[Model]
    id_value: int
    id_field: str
    model_key: str

    def __init__(self, model: type[Model], id_field: str, id_value: int | str):
        self.model = model
        self.id_field = id_field
        self.id_value = id_value
        self.model_key = self.get_model_key(model)

    def identifier(self):
        return f"{self.model_key}-{self.id_value}".lower()

    async def aexists(self) -> bool:
        return await self.model.objects.filter(
            **{self.id_field: self.id_value},
        ).aexists()

    def serialize(self) -> "SerializedRelation":
        return {
            "type": self.type(),
            "kwargs": {
                "model": self.model_key,
                "id_value": self.id_value,
                "id_field": self.id_field,
            },
        }

    @classmethod
    def deserialize(
        cls,
        model: str,
        id_field: str,
        id_value: int | str,
    ) -> "ModelRelation":
        return cls(apps.get_model(model), id_field, id_value)

    @staticmethod
    def get_model_key(model: type[Model]) -> str:
        return f"{model._meta.app_label}.{model._meta.model_name}"


class RelationType(Enum):
    MODEL = ModelRelation

    @classmethod
    def instance(cls, serialized_relation: "SerializedRelation") -> Relation:
        return cls[serialized_relation["type"]].value.deserialize(
            **serialized_relation["kwargs"],
        )


class SerializedRelation(TypedDict):
    type: str
    kwargs: dict
