from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_temporalio.client import init_client
from temporalio.exceptions import WorkflowAlreadyStartedError

from django_kafka.relations_resolver.resolver import RelationsResolver
from django_kafka.relations_resolver.temporal.relation import TemporalModelRelation
from django_kafka.temporalio.workflows import WaitingForRelation

if TYPE_CHECKING:
    from confluent_kafka import cimpl


class TemporalResolver(RelationsResolver):
    task_queue = settings.TEMPORAL_TASK_QUEUE
    relation_cls = TemporalModelRelation

    async def await_for_relation(self, msg: "cimpl.Message", relation: TemporalModelRelation):
        temporalio_client = await init_client()

        try:
            handle = await temporalio_client.start_workflow(
                WaitingForRelation.run,
                relation.serialize(),
                id=relation.workflow_id,
                task_queue=self.task_queue,
            )
        except WorkflowAlreadyStartedError:
            # cover concurrency
            handle = temporalio_client.get_workflow_handle(relation.workflow_id)

        await handle.signal(
            WaitingForRelation.add_message,
            {
                "topic": msg.topic(),
                "key": msg.key(),
                "value": msg.value(),
                "headers": msg.headers(),
            }
        )
