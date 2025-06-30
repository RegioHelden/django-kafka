from datetime import timedelta

from django_temporalio.registry import queue_workflows
from temporalio import workflow
from temporalio.common import RetryPolicy

from django_kafka.conf import settings
from django_kafka.relations_resolver.relation import SerializedRelation
from django_kafka.relations_resolver.temporalio.activities import (
    check_resolved_relations,
    resolve_relation,
)


@queue_workflows.register(settings.TEMPORAL_TASK_QUEUE)
@workflow.defn
class ResolveRelation:
    @workflow.run
    async def run(self, serialized_relation: "SerializedRelation"):
        await workflow.execute_activity(
            resolve_relation,
            serialized_relation,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(
                maximum_attempts=0,
                backoff_coefficient=1,
            ),  # every second indefinitely
            heartbeat_timeout=timedelta(seconds=60),
        )
        await workflow.wait_condition(workflow.all_handlers_finished)


@queue_workflows.register(settings.TEMPORAL_TASK_QUEUE)
@workflow.defn
class ResolveRelations:
    @workflow.run
    async def run(self):
        await workflow.execute_activity(
            check_resolved_relations,
            start_to_close_timeout=timedelta(minutes=20),
            heartbeat_timeout=timedelta(minutes=60),
        )
        await workflow.wait_condition(workflow.all_handlers_finished)
