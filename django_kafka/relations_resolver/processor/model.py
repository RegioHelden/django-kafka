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

    async def aadd_message(self, msg: "cimpl.Message", relation: "Relation"):
        await sync_to_async(self.model.objects.add_message)(msg, relation)

    async def adelete(self, relation: "Relation"):
        async_qs = await sync_to_async(self.model.objects.for_relation)(relation)
        await async_qs.adelete()

    async def aexists(self, relation: "Relation") -> bool:
        return await (
            await sync_to_async(self.model.objects.for_relation)(relation)
        ).aexists()

    async def aprocess_messages(self, relation: "Relation"):
        async for msg in await sync_to_async(self.model.objects.for_relation)(relation):
            kafka_msg = Message(
                key=msg.key,
                value=msg.value,
                topic=msg.topic,
                headers=msg.headers,
                timestamp=msg.timestamp,
                partition=msg.partition,
                offset=msg.offset,
            )
            try:
                topic: TopicConsumer = kafka.consumers.topic(kafka_msg.topic())
            except TopicNotRegisteredError:
                await msg.adelete()
                return

            if missing_relation := await self._aget_missing_relation(topic, kafka_msg):
                # doublecheck if there are other relations still missing
                # and send message for waiting into another queue
                await kafka.relations_resolver.await_for_relation(
                    kafka_msg,
                    missing_relation,
                )
                await msg.adelete()
                return

            await sync_to_async(
                kafka.consumers.topic(kafka_msg.topic()).consume,
            )(kafka_msg)
            await msg.adelete()

    async def _aget_missing_relation(
        self,
        topic: "TopicConsumer",
        msg: "cimpl.Message",
    ) -> Relation | None:
        for relation in topic.get_relations(msg):
            if not await relation.aexists():
                return relation
        return None

    async def ato_resolve(self) -> AsyncIterator["Relation"]:
        async for item in self.model.objects.aiter_relations_to_resolve(chunk_size=500):
            relation = await sync_to_async(item.relation)()
            if await relation.aexists():
                yield relation

    async def amark_resolving(self, relation: "Relation"):
        await sync_to_async(self.model.objects.mark_resolving)(relation)
