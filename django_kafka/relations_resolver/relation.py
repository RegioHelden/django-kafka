from abc import ABC, abstractmethod
from enum import Enum
from pydoc import locate
from typing import TYPE_CHECKING, TypedDict

from asgiref.sync import sync_to_async
from temporalio import workflow

from django_kafka import kafka

if TYPE_CHECKING:
    from confluent_kafka import cimpl

with workflow.unsafe.imports_passed_through():
    from django.contrib.contenttypes.models import ContentType
    from django.db.models import Model


class Relation(ABC):
    @abstractmethod
    def identifier(self) -> str:
        """"""

    @abstractmethod
    async def get(self):
        """"""

    @abstractmethod
    async def exists(self) -> bool:
        """"""

    @abstractmethod
    def serialize(self) -> dict:
        """"""

    @abstractmethod
    def deserialize(self, **kwargs) -> "Relation":
        """"""

    async def mark_resolving(self):
        await kafka.relations_resolver.processor.mark_resolving(self)

    async def aidentifier(self) -> str:
        return await sync_to_async(self.identifier)()

    def type(self) -> str:
        return RelationType(self.__class__).name

    async def has_waiting_messages(self) -> bool:
        return await kafka.relations_resolver.processor.exists(self)

    async def add_message(self, msg: "cimpl.Message"):
        await kafka.relations_resolver.processor.add_message(msg, self)

    async def resolve(self):
        await kafka.relations_resolver.processor.process_messages(self)


class ModelRelation(Relation):
    model: type[Model]
    id_value: int
    id_field: str
    model_path: str

    def __init__(self, model: type[Model], id_field: str, id_value: int | str):
        self.model = model
        self.id_field = id_field
        self.id_value = id_value
        self.model_path = f"{model.__module__}.{self.model.__name__}"

    def identifier(self):
        ct = ContentType.objects.get_for_model(self.model)
        return f"ct{ct.id}-{ct.app_label}-{ct.model}-{self.id_value}".lower()

    async def get(self) -> Model:
        return await self.model.objects.aget(**{self.id_field: self.id_value})

    async def exists(self) -> bool:
        return await self.model.objects.filter(
            **{self.id_field: self.id_value},
        ).aexists()

    def serialize(self) -> "SerializedRelation":
        return {
            "type": self.type(),
            "kwargs": {
                "model": self.model_path,
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
        return cls(locate(model), id_field, id_value)


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
