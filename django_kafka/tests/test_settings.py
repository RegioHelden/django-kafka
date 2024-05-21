from unittest.mock import patch

from django.test import TestCase, override_settings

from django_kafka.conf import DEFAULTS, SETTINGS_KEY, settings


class SettingsTestCase(TestCase):
    settings_keys = (
        "CLIENT_ID",
        "ERROR_HANDLER",
        "GLOBAL_CONFIG",
        "PRODUCER_CONFIG",
        "CONSUMER_CONFIG",
        "POLLING_FREQUENCY",
        "SCHEMA_REGISTRY",
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
            "POLLING_FREQUENCY": 0.5,
            "SCHEMA_REGISTRY": {
                "url": "https://schema-registry",
            },
        }
        with override_settings(**{SETTINGS_KEY: user_settings}):
            for key in self.settings_keys:
                self.assertEqual(getattr(settings, key), user_settings[key])
