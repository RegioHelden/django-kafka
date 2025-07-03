from unittest.mock import AsyncMock, MagicMock, Mock, patch

from django.test import SimpleTestCase

from django_kafka.relations_resolver.relation import Relation
from django_kafka.relations_resolver.resolver import RelationResolver
from django_kafka.tests.utils import message_mock


class RelationResolverTestCase(SimpleTestCase):
    async def test_await_for_relation(self):
        resolver = RelationResolver()
        msg = Mock()
        relation = MagicMock(spec=Relation)

        await resolver.await_for_relation(msg, relation)

        relation.aadd_message.assert_called_once_with(msg)

    @patch(
        "django_kafka.relations_resolver.resolver.RelationResolver.await_for_relation",
        new_callable=AsyncMock,
    )
    async def test_aresolve_relation_missing_wait(self, mock_await_for_relation):
        resolver = RelationResolver()
        msg = message_mock()

        relation = MagicMock(spec=Relation)
        # relations missing - send to waiting queue
        relation.aexists.return_value = False

        action = await resolver.aresolve([relation], msg)

        mock_await_for_relation.assert_called_once_with(msg, relation)
        relation.aexists.assert_called_once_with()
        self.assertEqual(action, RelationResolver.Action.SKIP)

        # relations exists and already has waiting messages - pause
        mock_await_for_relation.reset_mock()
        relation.reset_mock()
        relation.aexists.return_value = True
        relation.ahas_waiting_messages.return_value = True

        action = await resolver.aresolve([relation], msg)

        mock_await_for_relation.assert_not_called()
        relation.aexists.assert_called_once_with()
        relation.ahas_waiting_messages.assert_called_once_with()
        self.assertEqual(action, RelationResolver.Action.PAUSE)

        # relation exists, no waiting messages - continue
        mock_await_for_relation.reset_mock()
        relation.reset_mock()
        relation.aexists.return_value = True
        relation.ahas_waiting_messages.return_value = False

        action = await resolver.aresolve([relation], msg)

        mock_await_for_relation.assert_not_called()
        relation.aexists.assert_called_once_with()
        relation.ahas_waiting_messages.assert_called_once_with()
        self.assertEqual(action, RelationResolver.Action.CONTINUE)

    async def test_aresolve_relation(self):
        resolver = RelationResolver()
        relation = MagicMock(spec=Relation)

        await resolver.aresolve_relation(relation)

        relation.aresolve.assert_called_once_with()
