from unittest.mock import AsyncMock, Mock, patch

from django.test import TestCase

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
    async def test_add_message(self, mock_sync_to_async, mock_qs):
        msg = Mock()
        relation = Mock()

        await self.msg_processor.add_message(msg, relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.add_message)
        mock_sync_to_async().assert_called_once_with(msg, relation)

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_delete(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.delete(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        mock_sync_to_async().assert_called_once_with(relation)
        mock_sync_to_async().return_value.adelete.assert_called_once_with()

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_exists(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.exists(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.for_relation)
        mock_sync_to_async().assert_called_once_with(relation)
        mock_sync_to_async().return_value.aexists.assert_called_once_with()

    @patch("django_kafka.models.WaitingMessage.objects", spec=WaitingMessageQuerySet)
    @patch(
        "django_kafka.relations_resolver.processor.model.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_mark_resolving(self, mock_sync_to_async, mock_qs):
        relation = Mock()

        await self.msg_processor.mark_resolving(relation)

        mock_sync_to_async.assert_called_once_with(mock_qs.mark_resolving)
        mock_sync_to_async().assert_called_once_with(relation)

    async def test_to_resolve(self):
        not_to_resolve = [
            AsyncMock(**{"exists.return_value": False}),
        ]
        to_resolve = [
            AsyncMock(**{"exists.return_value": True}),
            AsyncMock(**{"exists.return_value": True}),
            AsyncMock(**{"exists.return_value": True}),
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
            result = [r async for r in self.msg_processor.to_resolve()]

        for relation in not_to_resolve:
            self.assertNotIn(relation, result)

        for relation in to_resolve:
            self.assertIn(relation, result)
