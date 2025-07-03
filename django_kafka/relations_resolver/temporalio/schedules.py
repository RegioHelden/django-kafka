from django_temporalio.registry import schedules
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)

from django_kafka.conf import settings
from django_kafka.relations_resolver.temporalio.workflows import ResolveRelations

schedules.register(
    "resolve-awaited-relations",
    Schedule(
        action=ScheduleActionStartWorkflow(
            ResolveRelations.run,
            id="resolve-awaited-relations",
            task_queue=settings.TEMPORAL_TASK_QUEUE,
        ),
        spec=ScheduleSpec(
            intervals=[
                ScheduleIntervalSpec(every=settings.RELATION_RESOLVER_DAEMON_INTERVAL),
            ],
        ),
    ),
)
