from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator

from asgiref.sync import async_to_sync

if TYPE_CHECKING:
    from confluent_kafka import cimpl
    from django_kafka.topic import TopicConsumer
    from django_kafka.relations_resolver.relation import ModelRelation


class RelationsResolver(ABC):
    @abstractmethod
    async def await_for_relation(self, msg: "cimpl.Message", relation):
        """
        Send the message to the comfortable place to wait for a relation.
        """

    async def aresolve(self, relations: Iterator["ModelRelation"], msg: "cimpl.Message") -> bool:
        """
        return True - resolved, when no dependencies are missing, the message can be consumed
        return False - not resolved, some dependency is missing, can't consume
        """
        for relation in relations:
            if not await relation.exists() or await relation.has_waiting_messages():
                await self.await_for_relation(msg, relation)
                return False  # not resolved, message sent to the workflow
        return True  # no dependencies missing, can consume

    def resolve(self, topic: "TopicConsumer", msg: "cimpl.Message") -> bool:
        return async_to_sync(self.aresolve)(topic, msg)
