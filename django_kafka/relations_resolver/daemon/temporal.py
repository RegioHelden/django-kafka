from typing import TYPE_CHECKING

from django_temporalio.client import init_client
from temporalio.common import RetryPolicy

from django_kafka.conf import settings
from django_kafka.relations_resolver.daemon import RelationResolverDaemon
from django_kafka.relations_resolver.temporalio.workflows import ResolveRelation

if TYPE_CHECKING:
    from django_kafka.relations_resolver.relation import Relation


class TemporalDaemon(RelationResolverDaemon):
    task_queue = settings.TEMPORAL_TASK_QUEUE

    async def aresolve_relation(self, relation: "Relation"):
        await (await init_client()).start_workflow(
            ResolveRelation.run,
            relation.serialize(),
            id=await relation.aidentifier(),
            task_queue=self.task_queue,
            retry_policy=RetryPolicy(maximum_attempts=0),  # indefinitely
        )
