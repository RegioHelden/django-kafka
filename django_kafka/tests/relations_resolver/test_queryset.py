import os

from asgiref.sync import sync_to_async
from django.test import TestCase
from django.utils import timezone

from django_kafka.models import WaitingMessage
from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.tests.relations_resolver.factories import (
    OrderFactory,
    WaitingMessageFactory,
)
from django_kafka.utils.message import Message, MessageTimestamp
from example.models import Order


class WaitingMessageQuerySetTestCase(TestCase):
    def test_add_message(self):
        kafka_timestamp = int(timezone.now().timestamp() * 1000)
        kafka_msg = Message(
            topic="topic",
            key=os.urandom(16),
            value=os.urandom(16),
            headers=[("key", b"value")],
            offset=1,
            partition=1,
            timestamp=(MessageTimestamp.CREATE_TIME.value, kafka_timestamp),
        )
        relation = ModelRelation(Order, id_field="id", id_value=1)
        waiting_msg = WaitingMessage.objects.add_message(kafka_msg, relation)

        self.assertEqual(waiting_msg.topic, kafka_msg.topic())
        self.assertEqual(waiting_msg.key, kafka_msg.key())
        self.assertEqual(waiting_msg.value, kafka_msg.value())
        self.assertEqual(waiting_msg.headers, kafka_msg.headers())
        self.assertEqual(waiting_msg.offset, kafka_msg.offset())
        self.assertEqual(waiting_msg.partition, kafka_msg.partition())
        self.assertEqual(
            waiting_msg.timestamp,
            (MessageTimestamp.CREATE_TIME.value, kafka_timestamp),
        )

    def test_for_relation(self):
        relation_a = ModelRelation(Order, id_field="id", id_value=100)
        relation_b = ModelRelation(Order, id_field="id", id_value=200)

        WaitingMessageFactory.create_batch(
            5,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field=relation_a.id_field,
            relation_id_value=relation_a.id_value,
            serialized_relation=relation_a.serialize(),
        )

        WaitingMessageFactory.create_batch(
            3,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field=relation_b.id_field,
            relation_id_value=relation_b.id_value,
            serialized_relation=relation_b.serialize(),
        )

        self.assertEqual(
            WaitingMessage.objects.for_relation(relation_a).count(),
            5,
        )
        self.assertEqual(
            WaitingMessage.objects.for_relation(relation_b).count(),
            3,
        )

    def test_relations(self):
        WaitingMessageFactory.create_batch(
            5,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field="id",
            relation_id_value=100,
            serialized_relation={},
        )

        WaitingMessageFactory.create_batch(
            3,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field="id",
            relation_id_value=200,
            serialized_relation={},
        )

        self.assertEqual(
            WaitingMessage.objects.relations().count(),
            2,
        )

    def test_waiting(self):
        for status in WaitingMessage.Status:
            WaitingMessageFactory.create_batch(
                3,
                status=status.value,
                relation_model_key=ModelRelation.get_model_key(Order),
                relation_id_field="id",
                relation_id_value=100,
                serialized_relation={},
            )

        results = WaitingMessage.objects.waiting()

        for msg in results:
            self.assertEqual(msg.status, WaitingMessage.Status.WAITING)

    def test_mark_resolving(self):
        relation_a = ModelRelation(Order, id_field="id", id_value=100)
        relation_b = ModelRelation(Order, id_field="id", id_value=200)

        WaitingMessageFactory.create_batch(
            3,
            status=WaitingMessage.Status.WAITING,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field=relation_a.id_field,
            relation_id_value=relation_a.id_value,
            serialized_relation={},
        )

        WaitingMessageFactory.create_batch(
            3,
            status=WaitingMessage.Status.WAITING,
            relation_model_key=ModelRelation.get_model_key(Order),
            relation_id_field=relation_b.id_field,
            relation_id_value=relation_b.id_value,
            serialized_relation={},
        )

        WaitingMessage.objects.mark_resolving(relation_a)

        for msg in WaitingMessage.objects.for_relation(relation_a):
            self.assertEqual(msg.status, WaitingMessage.Status.RESOLVING)

        for msg in WaitingMessage.objects.for_relation(relation_b):
            self.assertEqual(msg.status, WaitingMessage.Status.WAITING)

    async def test_aiter_relations_to_resolve(self):
        for status in WaitingMessage.Status:
            await sync_to_async(WaitingMessageFactory.create_batch)(
                3,
                status=status.value,
                relation_model_key=ModelRelation.get_model_key(Order),
                relation_id_field="id",
                relation_id_value=1000,
                serialized_relation={},
            )

        relation = await sync_to_async(OrderFactory.create)(id=1)
        for status in WaitingMessage.Status:
            await sync_to_async(WaitingMessageFactory.create_batch)(
                3,
                status=status.value,
                relation_model_key=ModelRelation.get_model_key(Order),
                relation_id_field="id",
                relation_id_value=relation.id,
                serialized_relation={},
            )

        results = [
            msg async for msg in WaitingMessage.objects.aiter_relations_to_resolve()
        ]

        self.assertEqual(len(results), 2)  # 1 existing and 1 non-existing
