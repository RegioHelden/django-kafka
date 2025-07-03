from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.relations_resolver.relation import Relation


class MessageProcessor(ABC):
    @abstractmethod
    async def aadd_message(self, msg: "cimpl.Message", relation: "Relation"): ...

    @abstractmethod
    async def adelete(self, relation: "Relation") -> bool: ...

    @abstractmethod
    async def aexists(self, relation: "Relation") -> bool: ...

    @abstractmethod
    async def aprocess_messages(self, relation: "Relation"): ...

    @abstractmethod
    async def ato_resolve(self) -> AsyncIterator["Relation"]: ...

    @abstractmethod
    async def amark_resolving(self): ...
