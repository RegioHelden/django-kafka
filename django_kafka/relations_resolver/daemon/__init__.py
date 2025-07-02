from abc import ABC, abstractmethod
from pydoc import locate
from typing import TYPE_CHECKING

from django_kafka import kafka
from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.relations_resolver.relation import Relation


class RelationResolverDaemon(ABC):
    @abstractmethod
    def resolve_relation(self, relation: "Relation"): ...

    async def resolve_relations(self):
        async for relation in kafka.relations_resolver.processor.to_resolve():
            await self._resolve_relation(relation)

    async def _resolve_relation(self, relation: "Relation"):
        await relation.mark_resolving()
        await self.resolve_relation(relation)


def get_daemon():
    if not (daemon_cls := locate(settings.RELATION_RESOLVER_DAEMON)):
        raise DjangoKafkaError(f"{settings.RELATION_RESOLVER_DAEMON} not found.")
    return daemon_cls()
