from django_kafka.conf import settings
from temporalio import workflow

from django_kafka.relations_resolver.temporal.message import Message
from django_kafka.temporalio.activities import consume_message, resolve_relation

from django_temporalio.registry import queue_workflows


@queue_workflows.register(settings.TEMPORAL_TASK_QUEUE)
@workflow.defn
class WaitingForRelation:
    _messages: list

    def __init__(self):
        self._messages = []

    @workflow.run
    async def run(self, relation_kwargs):
        # resolve_relation will keep failing and retrying indefinitely until the relation appears
        await workflow.execute_activity(
            resolve_relation,
            relation_kwargs,
            start_to_close_timeout=settings.TEMPORAL_START_TO_CLOSE_TIMEOUT,
        )
        # when resolve_relation succeeds, the messages may be consumed one by one
        while self._messages:
            await workflow.execute_activity(
                consume_message,
                self._messages[0],
                start_to_close_timeout=settings.TEMPORAL_START_TO_CLOSE_TIMEOUT,
            )
            self._messages.pop(0)

    @workflow.signal
    async def add_message(self, msg_args):
        self._messages.append(msg_args)

    @workflow.query
    def get_messages(self) -> list[Message]:
        return [Message(*args) for args in self._messages]
