from typing import ClassVar
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from django_kafka.conf import settings
from django_kafka.connect.client import KafkaConnectClient
from django_kafka.connect.connector import Connector
from django_kafka.exceptions import DjangoKafkaError


class MyConnector(Connector):
    config: ClassVar = {}


@patch("django_kafka.connect.client.KafkaConnectSession", new=Mock())
@patch.multiple("django_kafka.conf.settings", CONNECT_HOST="http://kafka-connect")
class ConnectorTestCase(SimpleTestCase):
    def test_name(self):
        class MyConnector2(Connector):
            name: ClassVar = "custom-name"
            config: ClassVar = {}

        self.assertEqual(MyConnector.name, "MyConnector")
        self.assertEqual(MyConnector2.name, "custom-name")

    @patch("django_kafka.connect.connector.KafkaConnectClient", spec=True)
    def test_init_request_session(self, mock_client):
        connector = MyConnector()

        mock_client.assert_called_with(
            host=settings.CONNECT_HOST,
            auth=settings.CONNECT_AUTH,
            retry=settings.CONNECT_RETRY,
            timeout=settings.CONNECT_REQUESTS_TIMEOUT,
        )
        self.assertIsInstance(connector.client, KafkaConnectClient)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.delete.return_value": Mock(ok=True),
        },
    )
    def test_delete_response_ok(self, mock_client):
        connector = MyConnector()

        result = connector.delete()

        self.assertIs(result, True)
        mock_client().delete.assert_called_once_with(connector.name)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.delete.return_value": Mock(ok=False, status_code=404),
        },
    )
    def test_delete_404(self, mock_client):
        connector = MyConnector()
        result = connector.delete()

        self.assertIs(result, False)
        mock_client().delete.assert_called_once_with(connector.name)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.delete.return_value": Mock(ok=False),
        },
    )
    def test_delete_response_not_ok_raise_error(self, mock_client):
        connector = MyConnector()

        with self.assertRaises(DjangoKafkaError):
            MyConnector().delete()

        mock_client().delete.assert_called_once_with(connector.name)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.submit.return_value": Mock(ok=True),
        },
    )
    def test_submit_response_ok(self, mock_client):
        connector = MyConnector()

        result = connector.submit()

        mock_client().update_or_create.assert_called_once_with(
            connector.name,
            connector.config,
        )
        response_json = mock_client().update_or_create().json()
        self.assertEqual(response_json, result)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.update_or_create.return_value": Mock(ok=False),
        },
    )
    def test_submit_response_not_ok_raise_error(self, mock_client):
        connector = MyConnector()

        with self.assertRaises(DjangoKafkaError):
            connector.submit()

        mock_client().update_or_create.assert_called_once_with(
            connector.name,
            connector.config,
        )

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.validate.return_value": Mock(ok=True),
        },
    )
    def test_is_valid_response_ok(self, mock_client):
        connector = MyConnector()

        result = connector.is_valid()

        mock_client().validate.assert_called_once_with(connector.config)
        response_ok = mock_client().validate().ok
        self.assertEqual(response_ok, result)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.validate.return_value": Mock(ok=False),
        },
    )
    def test_is_valid_response_not_ok_no_error(self, mock_client):
        connector = MyConnector()

        result = connector.is_valid()

        mock_client().validate.assert_called_once_with(connector.config)
        response_ok = mock_client().validate().ok
        self.assertIs(response_ok, False)
        self.assertEqual(result, response_ok)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.validate.return_value": Mock(ok=False),
        },
    )
    def test_is_valid_response_not_ok_raise_error(self, mock_client):
        connector = MyConnector()

        with self.assertRaises(DjangoKafkaError):
            connector.is_valid(raise_exception=True)

        mock_client().validate.assert_called_once_with(connector.config)

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.connector_status.return_value": Mock(
                ok=True,
                json=Mock(return_value={"connector": {"state": "RUNNING"}}),
            ),
        },
    )
    def test_status_response_ok(self, mock_client):
        connector = MyConnector()

        result = connector.status()

        mock_client().connector_status.assert_called_once_with(connector.name)
        self.assertEqual(
            result,
            mock_client().connector_status().json()["connector"]["state"],
        )

    @patch(
        "django_kafka.connect.connector.KafkaConnectClient",
        **{
            "return_value.connector_status.return_value": Mock(ok=False),
        },
    )
    def test_status_response_not_ok_raise_error(self, mock_client):
        connector = MyConnector()

        with self.assertRaises(DjangoKafkaError):
            connector.status()

        mock_client().connector_status.assert_called_once_with(connector.name)
