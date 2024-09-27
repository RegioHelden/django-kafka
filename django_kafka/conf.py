import socket

from django.conf import settings as django_settings

SETTINGS_KEY = "DJANGO_KAFKA"
DEFAULTS = {
    "CLIENT_ID": f"{socket.gethostname()}-python",
    "ERROR_HANDLER": "django_kafka.error_handlers.ClientErrorHandler",
    "GLOBAL_CONFIG": {},
    "PRODUCER_CONFIG": {},
    "CONSUMER_CONFIG": {},
    "RETRY_CONSUMER_CONFIG": {
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
        "topic.metadata.refresh.interval.ms": 10000,
    },
    "RETRY_TOPIC_SUFFIX": "retry",
    "DEAD_LETTER_TOPIC_SUFFIX": "dlt",
    "POLLING_FREQUENCY": 1,  # seconds
    "SCHEMA_REGISTRY": {},
    "CONNECT": {
        # Rest API of the kafka-connect instance
        "HOST": "",
        # `requests.auth.AuthBase` instance or tuple of (username, password) for Basic Auth
        "AUTH": None,
        # kwargs for `urllib3.util.retry.Retry` initialization
        "RETRY": dict(
            connect=5,
            read=5,
            status=5,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
        ),
        # `django_kafka.connect.client.KafkaConnectSession` would pass this value to every request method call
        "REQUESTS_TIMEOUT": 30,
    },
}


class Settings:
    @property
    def _settings(self):
        return getattr(django_settings, SETTINGS_KEY, {})

    def __getattr__(self, attr):
        if attr in self._settings:
            return self._settings[attr]

        if attr in DEFAULTS:
            return DEFAULTS[attr]

        raise AttributeError(f"Invalid setting: '{attr}'")


settings = Settings()
