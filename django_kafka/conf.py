import socket
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from django.conf import settings as django_settings

    from django_kafka.retry.settings import RetrySettings

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
    "REPRODUCER_CONFIG": {
        "namespace": None,
    },
    "RETRY_SETTINGS": None,
    "RETRY_TOPIC_SUFFIX": "retry",
    "DEAD_LETTER_TOPIC_SUFFIX": "dlt",
    "POLLING_FREQUENCY": 1,  # seconds
    "SCHEMA_REGISTRY": {},
    # Rest API of the kafka-connect instance
    "CONNECT_HOST": None,  # e.g. http://kafka-connect
    # `requests.auth.AuthBase` instance or tuple of (username, password) for Basic Auth
    "CONNECT_AUTH": None,
    # kwargs for `urllib3.util.retry.Retry` initialization
    "CONNECT_RETRY": {
        "connect": 5,
        "read": 5,
        "status": 5,
        "backoff_factor": 0.5,
        "status_forcelist": [502, 503, 504],
    },
    # `django_kafka.connect.client.KafkaConnectSession` would pass this
    # value to every request method call
    "CONNECT_REQUESTS_TIMEOUT": 30,
    "CONNECTOR_NAME_PREFIX": "",
    # relation resolver settings
    # ruff: noqa: E501
    "TEMPORAL_TASK_QUEUE": "django-kafka",
    "RELATION_RESOLVER": "django_kafka.relations_resolver.resolver.RelationResolver",
    "RELATION_RESOLVER_PROCESSOR": "django_kafka.relations_resolver.processor.model.ModelMessageProcessor",
    "RELATION_RESOLVER_DAEMON": "django_kafka.relations_resolver.daemon.temporal.TemporalDaemon",
    "RELATION_RESOLVER_DAEMON_INTERVAL": timedelta(seconds=5),
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

    def get_retry_settings(self) -> RetrySettings | None:
        return RetrySettings(**self.RETRY_SETTINGS) if self.RETRY_SETTINGS else None


settings = Settings()
