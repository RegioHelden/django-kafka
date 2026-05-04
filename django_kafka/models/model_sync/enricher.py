from typing import TYPE_CHECKING, Any

from confluent_kafka.serialization import MessageField

from django_kafka.conf import settings
from django_kafka.consumer import Consumer, Topics
from django_kafka.models.model_sync.transforms import TopicTransformsMixin
from django_kafka.schema.avro import AvroSchema, get_writer_schema
from django_kafka.topic.avro import AvroTopicProducer
from django_kafka.topic.reproducer import ReproduceTopic, TopicReproducer

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


class ModelSyncEnricher(TopicTransformsMixin, AvroTopicProducer, TopicReproducer):
    """
    Reproduces messages from the source topic to the enriched topic, applying
    the sync's `enrich_transforms` along the way. Pulls writer schemas from
    the registry so the enriched topic gets a schema derived from the source's,
    not a freshly-registered one.
    """

    transform_method_prefix = "enrich"

    def __init__(self, sync_cls: type["ModelSync"]):
        self.sync_cls = sync_cls
        self.reproduce_model = sync_cls.model
        super().__init__(transforms=sync_cls.enrich_transforms)

    @property
    def name(self) -> str:
        return self.sync_cls.get_enriched_topic()

    def _extended_schemas(
        self, key_schema: str | None, value_schema: str | None,
    ) -> tuple[str | None, str | None]:
        """
        Walk transforms once to derive both key and value schemas. Each schema
        passes through unchanged when no input is provided or no transforms
        affect it.
        """
        if not self.transforms:
            return key_schema, value_schema

        key_obj = AvroSchema.from_json(key_schema) if key_schema else None
        value_obj = AvroSchema.from_json(value_schema) if value_schema else None

        new_key_fields, new_value_fields = self.update_schema(
            self.sync_cls,
            key_obj.fields if key_obj else [],
            value_obj.fields if value_obj else [],
        )
        if key_obj is not None:
            key_obj.fields = new_key_fields
        if value_obj is not None:
            value_obj.fields = new_value_fields
        return (
            key_obj.to_json() if key_obj else None,
            value_obj.to_json() if value_obj else None,
        )

    def reproduce(
        self,
        msg_key: Any,
        msg_value: Any,
        is_deletion: bool,
        key_schema: str | None = None,
        value_schema: str | None = None,
    ) -> None:
        msg_key, msg_value = self.apply_transforms(self.sync_cls, msg_key, msg_value)

        # Forward writer schemas extended with our transforms' deltas so the
        # enriched topic uses the correct schema on both sides. Falls back
        # to producer defaults for keyless or non-Avro-encoded messages.
        new_key_schema, new_value_schema = self._extended_schemas(
            key_schema, value_schema,
        )
        key_kwargs = {"schema_str": new_key_schema} if new_key_schema else {}

        if is_deletion:
            self.produce(key=msg_key, value=None, key_serializer_kwargs=key_kwargs)
            return

        value_kwargs = {"schema_str": new_value_schema} if new_value_schema else {}

        self.produce(
            key=msg_key,
            value=msg_value,
            key_serializer_kwargs=key_kwargs,
            value_serializer_kwargs=value_kwargs,
        )

    @classmethod
    def for_sync(cls, sync_cls: type["ModelSync"]) -> ReproduceTopic:
        """
        Build the ReproduceTopic for `sync_cls` and register it with the
        enricher Consumer.

        Called by `ModelSyncRegistry._register_enricher` whenever a ModelSync
        with `enrich_transforms` is registered. The returned topic reads from
        the source topic, runs the enricher's `reproduce`, and produces to the
        enriched topic.
        """
        instance = cls(sync_cls)

        class _ReproduceTopic(ReproduceTopic):
            name = sync_cls.source_topic()
            reproducer = instance

            def consume(self, msg):
                msg_key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
                msg_value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
                if not self._skip_reproduce(msg_value):
                    self.reproducer.reproduce(
                        msg_key,
                        msg_value,
                        self._is_deletion(msg_value),
                        key_schema=get_writer_schema(msg.key()) if msg.key() else None,
                        value_schema=get_writer_schema(msg.value()) if msg.value() else None,
                    )

        return _ReproduceTopic()


class ModelSyncEnricherConsumer(Consumer):
    """
    Default Consumer that runs the enricher reproduce-topics for ModelSyncs.

    Topics are auto-discovered from the model_sync registry via the
    owner-aware `Topics` descriptor — each sync with `enrich_transforms`
    contributes a `ReproduceTopic` here. Used unless the sync overrides
    `enricher_consumer` or the project sets `MODEL_SYNC_ENRICHER_CONSUMER`.
    """

    topics = Topics()
    config = {
        "group.id": settings.MODEL_SYNC_ENRICHER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
    }
