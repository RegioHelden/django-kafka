from unittest.mock import patch

from django.test import override_settings, SimpleTestCase

from django_kafka.conf import DEFAULTS, SETTINGS_KEY, settings


class SettingsTestCase(SimpleTestCase):
    settings_keys = (
        "CLIENT_ID",
        "ERROR_HANDLER",
        "GLOBAL_CONFIG",
        "PRODUCER_CONFIG",
        "CONSUMER_CONFIG",
        "RETRY_CONSUMER_CONFIG",
        "RETRY_TOPIC_SUFFIX",
        "DEAD_LETTER_TOPIC_SUFFIX",
        "POLLING_FREQUENCY",
        "SCHEMA_REGISTRY",
        "CONNECT_HOST",
        "CONNECT_AUTH",
        "CONNECT_RETRY",
        "CONNECT_REQUESTS_TIMEOUT",
        "CONNECTOR_NAME_PREFIX",
    )

    @patch("django_kafka.consumer.ConfluentConsumer")
    def test_defaults(self, mock_consumer_client):
        # make sure defaults are assigned
        for key in self.settings_keys:
            self.assertEqual(getattr(settings, key), DEFAULTS[key])

    @patch("django_kafka.consumer.ConfluentConsumer")
    def test_user_settings(self, mock_consumer_client):
        # make sure settings defined by user pulled up
        user_settings = {
            "CLIENT_ID": "client-id",
            "ERROR_HANDLER": "error.handler.class",
            "GLOBAL_CONFIG": {"bootstrap.servers": "kafka1"},
            "PRODUCER_CONFIG": {
                "enable.idempotence": True,
            },
            "CONSUMER_CONFIG": {
                "group.id": "group-1",
            },
            "RETRY_CONSUMER_CONFIG": {
                "topic.metadata.refresh.interval.ms": 5000,
            },
            "RETRY_TOPIC_SUFFIX": "retry-extra",
            "DEAD_LETTER_TOPIC_SUFFIX": "dlt-extra",
            "POLLING_FREQUENCY": 0.5,
            "SCHEMA_REGISTRY": {
                "url": "https://schema-registry",
            },
            "CONNECT_HOST": "http://kafka-connect",
            "CONNECT_AUTH": ("user", "pass"),
            "CONNECT_RETRY": {
                "connect": 10,
                "read": 10,
                "status": 10,
                "backoff_factor": 0.1,
                "status_forcelist": [502, 503, 504],
            },
            "CONNECT_REQUESTS_TIMEOUT": 60,
            "CONNECTOR_NAME_PREFIX": "project_name"
        }
        with override_settings(**{SETTINGS_KEY: user_settings}):
            for key in self.settings_keys:
                self.assertEqual(getattr(settings, key), user_settings[key])
