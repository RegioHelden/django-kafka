from contextlib import contextmanager
from io import StringIO
from unittest.mock import DEFAULT, MagicMock, Mock, call, patch

from django.core.management import call_command
from django.test import SimpleTestCase
from requests.exceptions import RetryError

from django_kafka.connect.connector import Connector, ConnectorStatus
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.management.commands.kafka_connect import Command


@contextmanager
def patch_kafka_connectors(**attrs):
    attrs.setdefault("mark_for_removal", False)

    connector_instance = Mock(spec_set=Connector, **attrs)
    connector_class = Mock(spec_set=Connector, return_value=connector_instance)

    with patch(
        "django_kafka.kafka.connectors",
        {"connector.fake.path": connector_class},
    ):
        yield connector_instance


class KafkaConnectTestCase(SimpleTestCase):
    def setUp(self):
        self.connector_path = "connector.fake.path"
        self.command_stdout = StringIO()
        self.command = Command(stdout=self.command_stdout)
        self.command.connectors = [self.connector_path]

    @patch.multiple(
        "django_kafka.management.commands.kafka_connect.Command",
        handle_validate=DEFAULT,
        handle_publish=DEFAULT,
        handle_status=DEFAULT,
    )
    def test_execute_order(self, handle_validate, handle_publish, handle_status):
        # [Tracking order of calls](https://docs.python.org/3/library/unittest.mock-examples.html#tracking-order-of-calls-and-less-verbose-call-assertions)
        manager = MagicMock()
        manager.attach_mock(handle_validate, "handle_validate")
        manager.attach_mock(handle_publish, "handle_publish")
        manager.attach_mock(handle_status, "handle_status")

        call_command("kafka_connect", validate=True, publish=True, check_status=True)

        handle_validate.assert_called_once_with()
        handle_publish.assert_called_once_with()
        handle_status.assert_called_once_with()

        manager.assert_has_calls(
            [
                call.handle_validate(),
                call.handle_publish(),
                call.handle_status(),
            ],
            any_order=False,
        )

    def test_handle_validate(self):
        with patch_kafka_connectors() as connector:
            self.command.handle_validate()

        connector.is_valid.assert_called_once_with(raise_exception=True)

        self.assertFalse(self.command.has_failures)

    def test_handle_validate_when_marked_for_removal(self):
        with patch_kafka_connectors(mark_for_removal=True) as connector:
            self.command.handle_validate()

        connector.is_valid.assert_not_called()
        self.assertFalse(self.command.has_failures)

    def test_handle_validate__exceptions(self):
        with patch_kafka_connectors(
            **{"is_valid.side_effect": DjangoKafkaError},
        ) as connector:
            self.command.handle_validate()

        connector.is_valid.assert_called_once_with(raise_exception=True)
        self.assertTrue(self.command.has_failures)

    @patch("django_kafka.management.commands.kafka_connect.Command.handle_delete")
    @patch("django_kafka.management.commands.kafka_connect.Command.handle_submit")
    def test_handle_publish_marked_for_removal(
        self,
        mock_command_handle_submit,
        mock_command_handle_delete,
    ):
        with patch_kafka_connectors(mark_for_removal=True) as connector:
            self.command.handle_publish()

        mock_command_handle_submit.assert_not_called()
        mock_command_handle_delete.assert_called_once_with(connector)

    @patch("django_kafka.management.commands.kafka_connect.Command.handle_delete")
    @patch("django_kafka.management.commands.kafka_connect.Command.handle_submit")
    def test_handle_publish_not_marked_for_removal(
        self,
        mock_command_handle_submit,
        mock_command_handle_delete,
    ):
        with patch_kafka_connectors(mark_for_removal=False) as connector:
            self.command.handle_publish()

        mock_command_handle_submit.assert_called_once_with(connector)
        mock_command_handle_delete.assert_not_called()

    def test_handle_delete(self):
        with patch_kafka_connectors() as connector:
            self.command.handle_delete(connector)

        connector.delete.assert_called_once_with()
        self.assertFalse(self.command.has_failures)

    def test_handle_delete__django_kafka_error(self):
        with patch_kafka_connectors(
            **{"delete.side_effect": DjangoKafkaError},
        ) as connector:
            self.command.handle_delete(connector)

        connector.delete.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_delete__retry_error_error(self):
        with patch_kafka_connectors(**{"delete.side_effect": RetryError}) as connector:
            self.command.handle_delete(connector)

        connector.delete.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_submit(self):
        with patch_kafka_connectors() as connector:
            self.command.handle_submit(connector)

        self.assertFalse(self.command.has_failures)
        connector.submit.assert_called_once_with()

    def test_handle_submit__django_kafka_error(self):
        with patch_kafka_connectors(
            **{"submit.side_effect": DjangoKafkaError},
        ) as connector:
            self.command.handle_submit(connector)

        connector.submit.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_submit__retry_error_error(self):
        with patch_kafka_connectors(**{"submit.side_effect": RetryError}) as connector:
            self.command.handle_submit(connector)

        connector.submit.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_status__marked_for_removal(self):
        with patch_kafka_connectors(mark_for_removal=True) as connector:
            self.command.handle_status()

        connector.status.assert_not_called()

    def test_handle_status__running(self):
        with patch_kafka_connectors(
            **{"status.return_value": ConnectorStatus.RUNNING},
        ) as connector:
            self.command.handle_status()

        connector.status.assert_called_once_with()
        self.assertFalse(self.command.has_failures)

    def test_handle_status__paused(self):
        with patch_kafka_connectors(
            **{"status.return_value": ConnectorStatus.PAUSED},
        ) as connector:
            self.command.handle_status()

        connector.status.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_status__unassigned(self):
        with patch_kafka_connectors(
            **{"status.return_value": ConnectorStatus.UNASSIGNED},
        ) as connector:
            self.command.handle_status()

        # status UNASSIGNED is retried 3 times
        self.assertEqual(connector.status.call_count, 3)

        self.assertTrue(self.command.has_failures)

    def test_handle_status__django_kafka_error(self):
        with patch_kafka_connectors(
            **{"status.side_effect": DjangoKafkaError},
        ) as connector:
            self.command.handle_status()

        connector.status.assert_called_once_with()
        self.assertTrue(self.command.has_failures)

    def test_handle_status__retry_error(self):
        with patch_kafka_connectors(**{"status.side_effect": RetryError}) as connector:
            self.command.handle_status()

        connector.status.assert_called_once_with()
        self.assertTrue(self.command.has_failures)
