from asgiref.sync import sync_to_async

from django_kafka.exceptions import DjangoKafkaError
from django_temporalio.registry import queue_activities

from django_kafka.conf import settings
from temporalio import activity, workflow

from django_kafka.relations_resolver.temporal.message import Message

with workflow.unsafe.imports_passed_through():
    from django_kafka.relations_resolver.temporal.relation import TemporalModelRelation
    from django_kafka import kafka


@queue_activities.register(settings.TEMPORAL_TASK_QUEUE)
@activity.defn
async def resolve_relation(serialized_relation) -> bool:
    if not await TemporalModelRelation.deserialize(**serialized_relation).exists():
        raise DjangoKafkaError()
    return True


@queue_activities.register(settings.TEMPORAL_TASK_QUEUE)
@activity.defn
async def consume_message(msg_kwargs):
    msg = Message(**msg_kwargs)
    topic = kafka.consumers.topic(msg.topic())
    await sync_to_async(topic.consume)(msg)
