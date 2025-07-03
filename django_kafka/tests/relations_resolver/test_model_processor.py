from unittest.mock import AsyncMock, Mock, patch

from django.test import TestCase

from django_kafka.exceptions import TopicNotRegisteredError
from django_kafka.models import WaitingMessage, WaitingMessageQuerySet
from django_kafka.relations_resolver.processor.model import ModelMessageProcessor
from django_kafka.tests.utils import AsyncIteratorMock


class ModelMessageProcessorTestCase(TestCase):
    def setUp(self):
        self.msg_processor = ModelMessageProcessor()

    def test__init__(self):
        self.assertEqual(self.msg_processor.model, WaitingMessage)

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_aadd_message(self, mock_sync_to_async, mock_qs):
        msg = Mock()
        relation = Mock()

        await self.msg_processor.aadd_message(msg, relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.add_message)
        mock_sync_to_async().assert_called_once_with(msg, relation)

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_adelete(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.adelete(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        mock_sync_to_async().assert_called_once_with(relation)
        mock_sync_to_async().return_value.adelete.assert_called_once_with()

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_aexists(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.aexists(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        mock_sync_to_async().assert_called_once_with(relation)
        mock_sync_to_async().return_value.aexists.assert_called_once_with()

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_amark_resolving(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.amark_resolving(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.mark_resolving)
        mock_sync_to_async().assert_called_once_with(relation)

    async def test_ato_resolve(self):
        not_to_resolve = [
            AsyncMock(**{"aexists.return_value": False}),
        ]
        to_resolve = [
            AsyncMock(**{"aexists.return_value": True}),
            AsyncMock(**{"aexists.return_value": True}),
            AsyncMock(**{"aexists.return_value": True}),
        ]
        relations = [
            *not_to_resolve,
            *to_resolve,
        ]
        qs_items = [Mock(**{"relation.return_value": r}) for r in relations]

        with patch(
            "django_kafka.models.WaitingMessage.objects",
            aiter_relations_to_resolve=AsyncIteratorMock(qs_items),
        ):
            result = [r async for r in self.msg_processor.ato_resolve()]

        for relation in not_to_resolve:
            self.assertNotIn(relation, result)

        for relation in to_resolve:
            self.assertIn(relation, result)

    async def test__aget_missing_relation(self):
        existing_relations = [
            AsyncMock(**{"aexists.return_value": True}),
            AsyncMock(**{"aexists.return_value": True}),
            AsyncMock(**{"aexists.return_value": False}),
            AsyncMock(**{"aexists.return_value": True}),
            AsyncMock(**{"aexists.return_value": False}),
        ]
        topic = Mock(
            **{
                "get_relations.return_value": [
                    *existing_relations,
                ],
            },
        )
        msg = Mock()

        result = await self.msg_processor._aget_missing_relation(topic, msg)

        self.assertEqual(result, existing_relations[2])

    @patch("django_kafka.models.WaitingMessage.objects")
    @patch(
        "django_kafka.relations_resolver.processor.model.ModelMessageProcessor._aget_missing_relation",
    )
    @patch("django_kafka.kafka.consumers.topic")
    @patch("django_kafka.relations_resolver.processor.model.sync_to_async")
    async def test_aprocess_messages_topic_not_registered(
        self,
        mock_sync_to_async,
        mock_consumers_topic,
        mock__aget_missing_relation,
        mock_qs,
    ):
        relation = Mock()
        model_msg = AsyncMock()

        mock_sync_to_async.return_value = AsyncMock(
            return_value=AsyncIteratorMock([model_msg]),
        )
        mock_consumers_topic.side_effect = TopicNotRegisteredError

        await self.msg_processor.aprocess_messages(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        model_msg.adelete.assert_called_once_with()
        mock__aget_missing_relation.assert_not_called()

    @patch("django_kafka.models.WaitingMessage.objects")
    @patch(
        "django_kafka.relations_resolver.processor.model.ModelMessageProcessor._aget_missing_relation",
    )
    @patch("django_kafka.kafka.relations_resolver.await_for_relation")
    @patch("django_kafka.kafka.consumers.topic")
    @patch("django_kafka.relations_resolver.processor.model.Message")
    @patch("django_kafka.relations_resolver.processor.model.sync_to_async")
    async def test_aprocess_messages_some_relation_is_missing(
        self,
        mock_sync_to_async,
        mock_kafka_message,
        mock_consumers_topic,
        mock_await_for_relation,
        mock__aget_missing_relation,
        mock_qs,
    ):
        relation = Mock()
        model_msg = AsyncMock()
        missing_relation = Mock()

        mock_sync_to_async.return_value = AsyncMock(
            return_value=AsyncIteratorMock([model_msg]),
        )
        mock__aget_missing_relation.return_value = missing_relation

        await self.msg_processor.aprocess_messages(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        mock__aget_missing_relation.assert_called_once_with(
            mock_consumers_topic(),
            mock_kafka_message(),
        )
        mock_await_for_relation.assert_called_once_with(
            mock_kafka_message(),
            missing_relation,
        )
        model_msg.adelete.assert_called_once_with()

    @patch(
        "django_kafka.relations_resolver.processor.model.ModelMessageProcessor._aget_missing_relation",
    )
    @patch("django_kafka.kafka.relations_resolver.await_for_relation")
    @patch("django_kafka.kafka.consumers.topic")
    @patch("django_kafka.relations_resolver.processor.model.Message")
    @patch("django_kafka.relations_resolver.processor.model.sync_to_async")
    async def test_aprocess_messages_consume(
        self,
        mock_sync_to_async,
        mock_kafka_message,
        mock_consumers_topic,
        mock_await_for_relation,
        mock__aget_missing_relation,
    ):
        relation = Mock()
        model_msg = AsyncMock()

        sync_to_async_results = [
            # for model.objects.for_relation() call
            AsyncMock(return_value=AsyncIteratorMock([model_msg])),
            # for topic.consume() call
            AsyncMock(),
        ]

        mock_sync_to_async.side_effect = sync_to_async_results
        mock__aget_missing_relation.return_value = None

        await self.msg_processor.aprocess_messages(relation)

        mock_await_for_relation.assert_not_called()
        mock_sync_to_async.assert_any_call(mock_consumers_topic().consume)
        sync_to_async_results[1].assert_any_call(mock_kafka_message())
        model_msg.adelete.assert_called_once_with()
