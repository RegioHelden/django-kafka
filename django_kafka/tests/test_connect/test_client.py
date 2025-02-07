from unittest.mock import call, patch

from django.test import SimpleTestCase

from django_kafka.connect.client import KafkaConnectClient, KafkaConnectSession
from django_kafka.exceptions import DjangoKafkaError


class KafkaConnectSessionTestCase(SimpleTestCase):
    @patch("django_kafka.connect.client.KafkaConnectSession.mount_retry_adapter")
    def test_session_initializes_retry_not_provided(self, mock_mount_retry_adapter):
        KafkaConnectSession("http://localhost")
        mock_mount_retry_adapter.assert_not_called()

    @patch("django_kafka.connect.client.KafkaConnectSession.mount_retry_adapter")
    def test_session_initializes_retry(self, mock_mount_retry_adapter):
        retry_kwargs = {
            "connect": 1,
            "read": 1,
            "status": 1,
            "backoff_factor": 0.1,
            "status_forcelist": [500],
        }

        KafkaConnectSession("http://localhost", retry=retry_kwargs)

        mock_mount_retry_adapter.assert_called_once_with(retry_kwargs)

    @patch("django_kafka.connect.client.super")
    def test_session_request(self, mock_super):
        host = "http://localhost"
        request_path = "/path"

        session = KafkaConnectSession(host, timeout=10)

        # should use timeout value from the init
        session.request("GET", request_path)
        # should use the value of the provided timeout kwarg
        session.request("GET", request_path, timeout=20)

        # url was correctly constructed and right timeout was used
        mock_super().request.assert_has_calls(
            [
                call("GET", f"{host}{request_path}", timeout=10),
                call("GET", f"{host}{request_path}", timeout=20),
            ],
            any_order=False,
        )


class KafkaConnectClientTestCase(SimpleTestCase):
    @patch("django_kafka.connect.client.KafkaConnectSession", spec=True)
    def test_init_request_session(self, mock_session):
        init_args = "http://localhost", None, None, 10
        client = KafkaConnectClient(*init_args)
        mock_session.assert_called_once_with(*init_args)
        self.assertIsInstance(client._requests, KafkaConnectSession)

    @patch("django_kafka.connect.client.KafkaConnectSession")
    def test_validate_connector_class_not_in_config(self, mock_session):
        client = KafkaConnectClient("http://localhost")

        with self.assertRaises(DjangoKafkaError):
            client.validate({})

        mock_session.return_value.put.assert_not_called()

    @patch("django_kafka.connect.client.KafkaConnectSession")
    def test_validate_parse_connector_class(self, mock_session):
        client = KafkaConnectClient("http://localhost")
        config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        }
        connector_class_name = "PostgresConnector"

        result = client.validate(config)

        mock_session.return_value.put.assert_called_once_with(
            f"/connector-plugins/{connector_class_name}/config/validate",
            json=config,
        )
        self.assertEqual(result, mock_session.return_value.put.return_value)

    @patch("django_kafka.connect.client.KafkaConnectSession")
    def test_update_or_create(self, mock_session):
        client = KafkaConnectClient("http://localhost")
        connector_name = "connector_name"
        config = {"key": "value"}

        result = client.update_or_create(connector_name, config)

        mock_session.return_value.put.assert_called_once_with(
            f"/connectors/{connector_name}/config",
            json=config,
        )
        self.assertEqual(result, mock_session.return_value.put.return_value)

    @patch("django_kafka.connect.client.KafkaConnectSession")
    def test_delete(self, mock_session):
        client = KafkaConnectClient("http://localhost")
        connector_name = "connector_name"

        result = client.delete(connector_name)

        mock_session.return_value.delete.assert_called_once_with(
            f"/connectors/{connector_name}",
        )
        self.assertEqual(result, mock_session.return_value.delete.return_value)

    @patch("django_kafka.connect.client.KafkaConnectSession")
    def test_connector_status(self, mock_session):
        client = KafkaConnectClient("http://localhost")
        connector_name = "connector_name"

        result = client.connector_status(connector_name)

        mock_session.return_value.get.assert_called_once_with(
            f"/connectors/{connector_name}/status",
        )
        self.assertEqual(result, mock_session.return_value.get.return_value)
