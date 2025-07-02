from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from asgiref.sync import sync_to_async

from django_kafka import kafka
from django_kafka.exceptions import TopicNotRegisteredError
from django_kafka.relations_resolver.processor.base import MessageProcessor
from django_kafka.relations_resolver.relation import Relation
from django_kafka.utils.message import Message

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.topic import TopicConsumer


class ModelMessageProcessor(MessageProcessor):
    def __init__(self):
        from django_kafka.models import WaitingMessage

        self.model = WaitingMessage

    async def add_message(self, msg: "cimpl.Message", relation: "Relation"):
        await sync_to_async(self.model.objects.add_message)(msg, relation)

    async def delete(self, relation: "Relation"):
        await (await sync_to_async(self.model.objects.for_relation)(relation)).adelete()

    async def exists(self, relation: "Relation") -> bool:
        return await (
            await sync_to_async(self.model.objects.for_relation)(relation)
        ).aexists()

    async def process_messages(self, relation: "Relation"):
        async for m in await sync_to_async(self.model.objects.for_relation)(relation):
            msg = Message(
                key=m.key,
                value=m.value,
                topic=m.topic,
                headers=m.headers,
                timestamp=m.timestamp,
                partition=m.partition,
                offset=m.offset,
            )
            try:
                topic: TopicConsumer = kafka.consumers.topic(msg.topic())
            except TopicNotRegisteredError:
                await m.adelete()
                return

            if missing_relation := await self._get_missing_relation(topic, msg):
                # doublecheck if there are other relations still missing
                # and send message for waiting into another queue
                await kafka.relations_resolver.await_for_relation(msg, missing_relation)
                await m.adelete()
                return

            await sync_to_async(kafka.consumers.topic(msg.topic()).consume)(msg)
            await m.adelete()

    async def _get_missing_relation(
        self,
        topic: "TopicConsumer",
        msg: "cimpl.Message",
    ) -> bool:
        for relation in topic.get_relations(msg):
            if not await relation.exists():
                return True
        return False

    async def to_resolve(self) -> AsyncIterator["Relation"]:
        async for item in self.model.objects.aiter_relations_to_resolve(chunk_size=500):
            relation = await sync_to_async(item.relation)()
            if await relation.exists():
                yield relation

    async def mark_resolving(self, relation: "Relation"):
        await sync_to_async(self.model.objects.mark_resolving)(relation)
