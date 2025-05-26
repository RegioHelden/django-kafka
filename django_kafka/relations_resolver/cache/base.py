from abc import ABC, abstractmethod


class RelationResolverCache(ABC):
    @abstractmethod
    async def add(self, relation_identifier: str):
        """"""

    @abstractmethod
    async def delete(self, relation_identifier: str):
        """"""

    @abstractmethod
    async def exists(self, relation_identifier: str):
        """"""
