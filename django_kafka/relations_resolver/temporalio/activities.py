from datetime import timedelta

from django_temporalio.registry import queue_activities
from django_temporalio.utils import heartbeat
from temporalio import activity

from django_kafka import kafka
from django_kafka.conf import settings
from django_kafka.relations_resolver.daemon import get_daemon
from django_kafka.relations_resolver.relation import RelationType, SerializedRelation


@queue_activities.register(settings.TEMPORAL_TASK_QUEUE)
@activity.defn
async def resolve_relation(serialized_relation: SerializedRelation):
    async with heartbeat(timedelta(seconds=5)):
        await kafka.relations_resolver.aresolve_relation(
            RelationType.instance(serialized_relation),
        )


@queue_activities.register(settings.TEMPORAL_TASK_QUEUE)
@activity.defn
async def check_resolved_relations():
    async with heartbeat(timedelta(seconds=5)):
        await get_daemon().aresolve_relations()
