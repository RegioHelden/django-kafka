from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.relations_resolver.relation import Relation


class MessageProcessor(ABC):
    @abstractmethod
    async def add_msg(self, msg: "cimpl.Message", relation: "Relation"):
        """"""

    @abstractmethod
    async def delete(self, relation: "Relation") -> bool:
        """"""

    @abstractmethod
    async def exist(self, relation: "Relation") -> bool:
        """"""

    @abstractmethod
    async def process_messages(self, relation: "Relation"):
        """"""

    @abstractmethod
    async def to_resolve(self) -> AsyncIterator["Relation"]:
        """"""
