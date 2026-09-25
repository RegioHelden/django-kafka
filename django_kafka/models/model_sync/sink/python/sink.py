from typing import TYPE_CHECKING

from django.apps import apps
from django.db.models import ForeignKey

from django_kafka.conf import settings
from django_kafka.models.model_sync.sink.base import Sink
from django_kafka.models.model_sync.sink.python.topic import (
    PythonSinkAvroTopicConsumer,
    PythonSinkTopicBase,
)
from django_kafka.models.model_sync.transforms import RelationTransform

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.db.models import Model

    from django_kafka.models.model_sync.sync import ModelSync


class PythonSink(Sink):
    """
    Base for Python topic consumer sinks.

    topic_consumer_class: a TopicConsumer that already inherits from
        `PythonSinkTopicBase`. The default for `PythonAvroSink` is
        `PythonAvroTopicConsumer`. Users with custom deserialization
        provide their own combined class.
    consumer: dotted path to the Consumer class this sink belongs to.
        Falls back to MODEL_SYNC_CONSUMER setting. Required — one of the
        two must be set, otherwise registration raises.
    FK relations are auto-detected from the model's non-nullable, non-blank
    fields and appended to `consume_transforms` as `RelationTransform`s, so
    they resolve after the declared steps. Declare a `RelationTransform`
    yourself to place it earlier, to look a relation up by a non-pk field,
    or to resolve a nullable/blank FK that auto-detection skips.
    """

    topic_consumer_class: type[PythonSinkTopicBase] | None = None

    def __init__(
        self,
        instance: "ModelSync | None" = None,
        topic_consumer_class: type[PythonSinkTopicBase] | None = None,
        consumer: str | None = None,
    ):
        super().__init__(
            instance=instance,
            topic_consumer_class=topic_consumer_class,
            consumer=consumer,
        )
        if topic_consumer_class is not None:
            self.topic_consumer_class = topic_consumer_class
        self.consumer = consumer

    def _detect_relation_transforms(self) -> "Iterator[RelationTransform]":
        declared = self.instance.consume_transforms
        relations = [t for t in declared if isinstance(t, RelationTransform)]
        content_type = self._content_type_model()

        for field in self.instance.model._meta.fields:
            if any(
                [
                    not isinstance(field, ForeignKey),
                    field.remote_field and field.remote_field.parent_link,
                    # skipped by default, declare a RelationTransform to opt in:
                    # content types are created by migrations, never synced in
                    field.related_model is content_type,
                    field.null,
                    field.blank,
                ],
            ):
                continue

            # detecting a fk that is already declared waits on the same row twice
            if any(relation.resolves(field) for relation in relations):
                continue

            # wait-only: no target means no lookup
            yield RelationTransform(
                source=field.attname,
                model=field.related_model,
                id_field="id",
            )

    @staticmethod
    def _content_type_model() -> "type[Model] | None":
        try:
            return apps.get_model("contenttypes", "ContentType")
        except LookupError:
            return None

    @property
    def consumer_path(self) -> str:
        path = self.consumer or getattr(settings, "MODEL_SYNC_CONSUMER", None)
        if not path:
            raise ValueError(
                "PythonSink requires a consumer: pass `consumer=` to the sink "
                "or set MODEL_SYNC_CONSUMER in DJANGO_KAFKA settings.",
            )
        return path

    def make_topic(self) -> PythonSinkTopicBase:
        sync = self.instance
        topic_name = (
            sync.get_enriched_topic() if sync.has_enrich() else sync.source_topic()
        )
        return self.topic_consumer_class(
            name=topic_name,
            model=sync.model,
            sync=sync,
            transforms=[
                *sync.consume_transforms,
                *self._detect_relation_transforms(),
            ],
        )


class PythonAvroSink(PythonSink):
    """Python topic consumer sink with Avro deserialization."""

    topic_consumer_class = PythonSinkAvroTopicConsumer
