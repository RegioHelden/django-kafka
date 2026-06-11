from typing import TYPE_CHECKING

from django.db.models import ForeignKey

from django_kafka.conf import settings
from django_kafka.models.model_sync.sink.base import Sink
from django_kafka.models.model_sync.sink.python.topic import (
    PythonSinkAvroTopicConsumer,
    PythonSinkTopicBase,
    Relation,
)

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
    relations: Relation declarations for FK resolution. Auto-detected
        from the model's non-nullable, non-blank FK fields. An explicit
        entry with `fk` set replaces the auto-detected entry for that
        FK field, and also forces inclusion of nullable/blank FKs that
        would otherwise be skipped.
    """

    topic_consumer_class: type[PythonSinkTopicBase] | None = None

    def __init__(
        self,
        instance: "ModelSync | None" = None,
        topic_consumer_class: type[PythonSinkTopicBase] | None = None,
        consumer: str | None = None,
        relations: list[Relation] | None = None,
    ):
        super().__init__(
            instance=instance,
            topic_consumer_class=topic_consumer_class,
            consumer=consumer,
            relations=relations,
        )
        if topic_consumer_class is not None:
            self.topic_consumer_class = topic_consumer_class
        self.consumer = consumer
        self.relations = relations

    def _auto_detect_relations(self, model: "type[Model]") -> "Iterator[Relation]":
        for field in model._meta.fields:
            if any(
                [
                    not isinstance(field, ForeignKey),
                    field.remote_field and field.remote_field.parent_link,
                ],
            ):
                continue

            for relation in self.relations or []:
                if relation.fk == field.name:
                    yield relation
                    break
            else:
                if any([field.null, field.blank]):
                    continue
                yield Relation(
                    model=field.related_model,
                    id_field="id",
                    value_field=field.attname,
                )

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
        relations = list(self._auto_detect_relations(sync.model))
        return self.topic_consumer_class(
            name=topic_name,
            model=sync.model,
            sync=sync,
            relations=relations,
            transforms=[
                *[t for r in relations if (t := r.to_transform())],
                *sync.consume_transforms,
            ],
        )


class PythonAvroSink(PythonSink):
    """Python topic consumer sink with Avro deserialization."""

    topic_consumer_class = PythonSinkAvroTopicConsumer
