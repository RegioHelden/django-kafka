from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.models.model_sync.sink.base import Sink
from django_kafka.models.model_sync.sink.python.topic import (
    PythonSinkAvroTopicConsumer,
    PythonSinkTopicBase,
    Relation,
)

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


class PythonSink(Sink):
    """
    Base for Python topic consumer sinks.

    topic_consumer_class: a TopicConsumer that already inherits from
        `PythonSinkTopicBase`. The default for `PythonAvroSink` is
        `PythonAvroTopicConsumer`. Users with custom deserialization
        provide their own combined class.
    consumer: dotted path to the Consumer class this sink belongs to.
        Falls back to MODEL_SYNC_CONSUMER setting.
    relations: Relation declarations for FK resolution.
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
        self.relations = relations or []

    @property
    def consumer_path(self) -> str | None:
        return self.consumer or getattr(settings, "MODEL_SYNC_CONSUMER", None)

    def get_transforms(self):
        """
        Transforms this sink contributes to the consume pipeline.

        Used by `ModelSync._validate` to check transform eligibility
        against `fields`. Currently: FK transforms derived from each
        Relation that declares `fk`.
        """
        return [t for r in self.relations if (t := r.to_transform())]

    def make_topic(self) -> PythonSinkTopicBase:
        sync = self.instance
        topic_name = (
            sync.get_enriched_topic() if sync.has_enrich() else sync.source_topic()
        )
        return self.topic_consumer_class(
            name=topic_name,
            model=sync.model,
            sync=sync,
            relations=self.relations,
            transforms=[*self.get_transforms(), *sync.consume_transforms],
        )


class PythonAvroSink(PythonSink):
    """Python topic consumer sink with Avro deserialization."""

    topic_consumer_class = PythonSinkAvroTopicConsumer
