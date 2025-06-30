import logging
from collections.abc import Iterator
from enum import IntEnum
from pydoc import locate
from typing import TYPE_CHECKING

from asgiref.sync import async_to_sync

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.relations_resolver.processor.base import MessageProcessor
    from django_kafka.relations_resolver.relation import Relation

logger = logging.getLogger(__name__)


class RelationResolver:
    processor: "MessageProcessor"

    class Action(IntEnum):
        CONTINUE = 1
        SKIP = 2
        PAUSE = 3

    def __init__(self):
        if not (processor_cls := locate(settings.RELATION_RESOLVER_PROCESSOR)):
            raise DjangoKafkaError(f"{settings.RELATION_RESOLVER_PROCESSOR} not found.")
        self.processor = processor_cls()

    async def await_for_relation(self, msg: "cimpl.Message", relation: "Relation"):
        """
        Send the message to the comfortable place to wait for a relation.
        """
        await relation.add_message(msg)

    async def aresolve(
        self,
        relations: Iterator["Relation"],
        msg: "cimpl.Message",
    ) -> Action:
        logger.debug("Check for missing relations.")
        for relation in relations:
            if not await relation.exists():
                logger.debug("Relation is missing - send message to wait.")
                await self.await_for_relation(msg, relation)
                return self.Action.SKIP

            if await relation.has_waiting_messages():
                logger.debug("Relation exists but has waiting messages.")
                return self.Action.PAUSE

        logger.debug("No relations missing.")
        return self.Action.CONTINUE

    def resolve(self, relations: Iterator["Relation"], msg: "cimpl.Message") -> Action:
        return async_to_sync(self.aresolve)(relations, msg)

    async def process_resolved(self):
        async for relation in self.processor.to_resolve():
            await self.resolve_relation(relation)

    async def resolve_relation(self, relation: "Relation"):
        await relation.resolve()
