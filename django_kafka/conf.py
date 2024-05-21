import socket

from django.conf import settings as django_settings

SETTINGS_KEY = "DJANGO_KAFKA"
DEFAULTS = {
    "CLIENT_ID": f"{socket.gethostname()}-python",
    "ERROR_HANDLER": "django_kafka.error_handlers.ClientErrorHandler",
    "GLOBAL_CONFIG": {},
    "PRODUCER_CONFIG": {},
    "CONSUMER_CONFIG": {},
    "POLLING_FREQUENCY": 1,  # seconds
    "SCHEMA_REGISTRY": {},
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
