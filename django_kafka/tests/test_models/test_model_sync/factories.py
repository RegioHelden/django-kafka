import uuid
from typing import TypedDict
from unittest import mock

from django.db import models

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.models.model_sync import EnricherTransform, ModelSync
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource


class _EnrichResult(TypedDict):
    brand_name: str


class SimpleModel(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_model_sync"

    def __str__(self):
        return self.name


class RelatedModel(models.Model):  # noqa: DJ008
    class Meta:
        app_label = "test_model_sync"


class ModelWithFK(models.Model):  # noqa: DJ008
    related = models.ForeignKey(RelatedModel, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_model_sync"


class ModelWithFKChild(ModelWithFK):  # noqa: DJ008
    """MTI child — inherits `related` FK from ModelWithFK's table."""

    class Meta:
        app_label = "test_model_sync"


class ModelWithNullableFK(models.Model):  # noqa: DJ008
    nullable_related = models.ForeignKey(
        RelatedModel,
        null=True,
        on_delete=models.SET_NULL,
    )

    class Meta:
        app_label = "test_model_sync"


class BidirectionalModel(KafkaConnectSkipModel):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_model_sync"

    def __str__(self):
        return self.name


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


def make_sync_with_enrich(registry, **attrs):
    def enrich(key, value) -> _EnrichResult:
        return {"brand_name": "brand"}

    attrs.setdefault("enrich", staticmethod(enrich))
    attrs.setdefault("enrich_transforms", [EnricherTransform()])
    return make_sync(registry, **attrs)
