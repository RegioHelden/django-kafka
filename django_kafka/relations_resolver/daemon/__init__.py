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
    async def aresolve_relation(self, relation: "Relation"): ...

    async def aresolve_relations(self):
        batch_size = settings.RELATION_RESOLVER_DAEMON_BATCH_SIZE
        dispatched = 0
        async for relation in kafka.relations_resolver.processor.ato_resolve():
            await self._aresolve_relation(relation)
            dispatched += 1
            # every dispatch starts work elsewhere, so a whole backlog at once
            # queues up faster than it can be worked off
            if batch_size and dispatched >= batch_size:
                return

    async def _aresolve_relation(self, relation: "Relation"):
        await relation.amark_resolving()
        await self.aresolve_relation(relation)


def get_daemon():
    if not (daemon_cls := locate(settings.RELATION_RESOLVER_DAEMON)):
        raise DjangoKafkaError(f"{settings.RELATION_RESOLVER_DAEMON} not found.")
    return daemon_cls()
