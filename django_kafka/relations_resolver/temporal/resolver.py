from typing import TYPE_CHECKING

from django_temporalio.client import init_client
from temporalio.common import RetryPolicy

from django_kafka.conf import settings
from django_kafka.relations_resolver.resolver import RelationResolver
from django_kafka.relations_resolver.temporal.relation import TemporalModelRelation
from django_kafka.temporalio.workflows import MessagesAwaitingRelation

if TYPE_CHECKING:
    from confluent_kafka import cimpl


class TemporalResolver(RelationResolver):
    task_queue = settings.TEMPORAL_TASK_QUEUE
    relation_cls = TemporalModelRelation

    async def await_for_relation(
        self,
        msg: "cimpl.Message",
        relation: TemporalModelRelation,
    ):
        client = await init_client()

        if await relation.has_waiting_messages():
            handle = client.get_workflow_handle(relation.identifier)
        else:
            handle = await client.start_workflow(
                MessagesAwaitingRelation.run,
                relation.serialize(),
                id=relation.identifier,
                task_queue=self.task_queue,
                retry_policy=RetryPolicy(maximum_attempts=0),  # indefinitely
            )
            await relation.mark_as_awaited()

        await handle.signal(
            MessagesAwaitingRelation.add_message,
            {
                "topic": msg.topic(),
                "key": msg.key(),
                "value": msg.value(),
                "headers": msg.headers(),
            },
        )
