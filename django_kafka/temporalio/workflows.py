from django_temporalio.registry import queue_workflows
from temporalio import workflow
from temporalio.common import RetryPolicy

from django_kafka.conf import settings
from django_kafka.temporalio.activities import (
    cleanup,
    consume_message,
    resolve_relation,
)


@queue_workflows.register(settings.TEMPORAL_TASK_QUEUE)
@workflow.defn
class MessagesAwaitingRelation:
    _messages: list

    def __init__(self):
        self._messages = []

    @workflow.run
    async def run(self, relation_kwargs):
        # keeps failing and retrying indefinitely until the relation appears
        await workflow.execute_activity(
            resolve_relation,
            relation_kwargs,
            start_to_close_timeout=settings.TEMPORAL_START_TO_CLOSE_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=0),  # indefinitely
        )
        # when resolve_relation succeeds, the messages may be consumed one by one
        while self._messages:
            await workflow.execute_activity(
                consume_message,
                self._messages[0],
                start_to_close_timeout=settings.TEMPORAL_START_TO_CLOSE_TIMEOUT,
            )
            self._messages.pop(0)

        await workflow.execute_activity(
            cleanup,
            relation_kwargs,
            start_to_close_timeout=settings.TEMPORAL_START_TO_CLOSE_TIMEOUT,
        )

    @workflow.signal
    async def add_message(self, msg_args):
        self._messages.append(msg_args)
