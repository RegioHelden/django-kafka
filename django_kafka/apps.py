from django.apps import AppConfig


class DjangoKafkaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_kafka"

    def ready(self):
        super().ready()
        self.module.autodiscover()
