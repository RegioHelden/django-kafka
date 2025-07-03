from unittest.mock import AsyncMock, Mock, PropertyMock, patch

from django.test import SimpleTestCase

from django_kafka.relations_resolver.processor.base import MessageProcessor
from django_kafka.relations_resolver.relation import Relation


@patch.multiple(
    "django_kafka.relations_resolver.relation.Relation",
    __abstractmethods__=set(),
)
@patch(
    "django_kafka.kafka.relations_resolver.processor",
    new_callable=PropertyMock(
        return_value=AsyncMock(spec=MessageProcessor, __abstractmethods__=set()),
    ),
)
class RelationTestCase(SimpleTestCase):
    async def test_amark_resolving_uses_msg_processor(self, mock_msg_processor):
        relation = Relation()
        await relation.amark_resolving()
        mock_msg_processor.amark_resolving.assert_called_once_with(relation)

    @patch(
        "django_kafka.relations_resolver.relation.sync_to_async",
        return_value=AsyncMock(),
    )
    async def test_aidentifier_converts_identifier_method(
        self,
        mock_sync_to_async,
        mock_msg_processor,
    ):
        relation = Relation()
        await relation.aidentifier()
        mock_sync_to_async.assert_called_once_with(relation.identifier)

    @patch("django_kafka.relations_resolver.relation.RelationType")
    def test_type_returns_relation_type_name(
        self,
        mock_relation_type,
        mock_msg_processor,
    ):
        self.assertEqual(Relation().type(), mock_relation_type.return_value.name)

    async def test_ahas_waiting_messages_uses_msg_processor(self, mock_msg_processor):
        relation = Relation()
        await relation.ahas_waiting_messages()
        mock_msg_processor.aexists.assert_called_once_with(relation)

    async def test_aadd_message_uses_msg_processor(self, mock_msg_processor):
        relation = Relation()
        msg = Mock()
        await relation.aadd_message(msg)
        mock_msg_processor.aadd_message.assert_called_once_with(msg, relation)

    async def test_resolve_uses_msg_processor(self, mock_msg_processor):
        relation = Relation()
        await relation.aresolve()
        mock_msg_processor.aprocess_messages.assert_called_once_with(relation)
