import uuid
from unittest import mock

from django.db import models

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.models.model_sync import ModelSync
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource


class SimpleModel(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_model_sync"


class BidirectionalModel(KafkaConnectSkipModel):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_model_sync"


@mock.patch(
    "django_kafka.conf.settings.MODEL_SYNC_SOURCE_CONNECTOR",
    new="test.connectors.TestConnector",
)
@mock.patch(
    "django_kafka.models.model_sync.registry.kafka",
    new=mock.MagicMock(),
)
def make_sync(registry, model=None, **attrs):
    attrs.setdefault("model", model or SimpleModel)
    attrs.setdefault("source", DbzPostgresSource())
    with mock.patch(
        "django_kafka.models.model_sync.sync.model_sync_registry",
        registry,
    ):
        return type(f"TestSync_{uuid.uuid4().hex[:8]}", (ModelSync,), attrs)
