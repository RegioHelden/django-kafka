import logging
from collections.abc import Iterator
from enum import IntEnum
from typing import TYPE_CHECKING

from asgiref.sync import async_to_sync

from django_kafka.relations_resolver.processor import get_message_processor

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
        self.processor = get_message_processor()

    async def await_for_relation(self, msg: "cimpl.Message", relation: "Relation"):
        """
        Send the message to the comfortable place to wait for a relation.
        """
        await relation.aadd_message(msg)

    async def awith_predecessors(
        self,
        relations: Iterator["Relation"],
        msg: "cimpl.Message",
    ) -> list["Relation"]:
        """
        Augments the message's own relations with any predecessor relations
        still queued for the same (topic, key).
        """
        predecessor_relations = await self.processor.awaiting_relations_for(msg)
        # dict.fromkeys dedups while preserving first-occurrence order
        # (own relations first, then predecessors).
        return list(dict.fromkeys([*relations, *predecessor_relations]))

    async def aresolve(
        self,
        relations: Iterator["Relation"],
        msg: "cimpl.Message",
    ) -> Action:
        logger.debug("Check for missing relations.")

        if msg.value() is None:
            # A tombstone supersedes anything queued for the same (topic, key)
            # and the deletion itself needs no relations, so it must never wait:
            # its predecessors may depend on a relation that was cascade-deleted
            # upstream and will never arrive.
            await self.processor.adiscard_messages(msg)
            if await self.processor.awaiting_relations_for(msg):
                # messages that survived the discard are being replayed by the
                # daemon right now - the deletion must apply strictly after them
                logger.debug("Tombstone - predecessors are resolving, pause.")
                return self.Action.PAUSE
            logger.debug("Tombstone - queued predecessors discarded, consume.")
            return self.Action.CONTINUE

        for relation in await self.awith_predecessors(relations, msg):
            if not await relation.aexists():
                logger.debug("Relation is missing - send message to wait.")
                await self.await_for_relation(msg, relation)
                return self.Action.SKIP

            if await relation.ahas_waiting_messages():
                logger.debug("Relation exists but has waiting messages.")
                return self.Action.PAUSE

        logger.debug("No relations missing.")
        return self.Action.CONTINUE

    def resolve(self, relations: Iterator["Relation"], msg: "cimpl.Message") -> Action:
        return async_to_sync(self.aresolve)(relations, msg)

    async def aresolve_relation(self, relation: "Relation"):
        await relation.aresolve()
