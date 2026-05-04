from django.db.models import Model

from django_kafka.conf import settings
from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.consumer import Consumer, Topics
from django_kafka.models.model_sync.fields import ExcludeFields, IncludeFields
from django_kafka.models.model_sync.registry import model_sync_registry
from django_kafka.models.model_sync.sink import ConnectorSink
from django_kafka.models.model_sync.sink.python import PythonSink
from django_kafka.models.model_sync.source import ConnectorSource
from django_kafka.models.model_sync.transforms import Transform


class ModelSync:
    """
    Declarative bidirectional sync between a Django model and a Kafka topic.

    Subclasses describe the data flow by setting some combination of:
        - `source`: emits CDC/change events to the topic (e.g. DbzPostgresSource).
        - `sink`: writes incoming messages back into the model (DbzJdbcSink for
          a JDBC connector, PythonAvroSink for a consumer-driven sink).
        - `enrich_transforms` / `consume_transforms`: per-message Transforms
          that run in the enricher or in the sink consumer. See `transforms.py`.

    Source-only, sink-only, and bidirectional configurations are all valid.
    Bidirectional setups require `KafkaConnectSkipModel` (for kafka_skip-based
    loop prevention) and an explicit `topic` (so both ends agree on the name).

    Subclasses are auto-registered via `__init_subclass__`. The registry then
    wires up:
        - source ModelSyncs into the configured Kafka Connect source connector,
        - ConnectorSink ModelSyncs as standalone Kafka Connect sink connectors,
        - PythonSink ModelSyncs as topics on `MODEL_SYNC_CONSUMER`,
        - syncs with `enrich_transforms` as ReproduceTopics on the enricher
          consumer.

    Topic naming follows `<MODEL_SYNC_TOPIC_PREFIX>.<MODEL_SYNC_DB_SCHEMA>.<table>`
    unless `topic` is set explicitly. Enriched topics get `enriched.` injected.
    """

    model: type[Model]
    source: ConnectorSource | None = None
    sink: ConnectorSink | PythonSink | None = None
    # base topic name; source writes here, enricher reads from here
    topic: str | None = None
    # enriched topic name; overrides auto-generated enriched topic
    enriched_topic: str | None = None
    # column include/exclude list propagated to source/sink config
    fields: IncludeFields | ExcludeFields | None = None
    # dotted path to a Consumer that should run this sync's enricher topic
    # (defaults to MODEL_SYNC_ENRICHER_CONSUMER setting)
    enricher_consumer: str | None = None
    # transforms applied to outgoing messages (run by the enricher)
    enrich_transforms: list[Transform] = []
    # transforms applied to incoming messages (run by the sink consumer)
    consume_transforms: list[Transform] = []

    def __init_subclass__(cls, **kwargs) -> None:
        # Hook into class creation so users only need to subclass ModelSync —
        # validation and registration happen automatically.
        super().__init_subclass__(**kwargs)
        cls._validate()
        model_sync_registry.register(cls)

    @classmethod
    def _validate(cls):
        if not issubclass(cls.model, Model):
            raise TypeError(
                f"{cls.__name__}.model must be a Django Model class, got {cls.model}",
            )

        if cls.sink is None and cls.source is None:
            raise ValueError(
                f"{cls.__name__} must define at least one of 'sink' or 'source'.",
            )

        if cls.source is not None and cls.sink is not None:
            if not issubclass(cls.model, KafkaConnectSkipModel):
                raise ValueError(
                    f"{cls.__name__} defines both 'source' and "
                    f"'sink' (bidirectional sync) but "
                    f"{cls.model.__name__} does not inherit "
                    f"from KafkaConnectSkipModel.",
                )
            if not cls.topic:
                raise ValueError(
                    f"{cls.__name__} defines both 'source' and "
                    f"'sink' (bidirectional sync) but 'topic' "
                    f"is not set.",
                )


    @classmethod
    def is_bidirectional(cls) -> bool:
        return cls.source is not None and cls.sink is not None

    @classmethod
    def has_enrich(cls) -> bool:
        return bool(cls.enrich_transforms)

    @classmethod
    def source_topic(cls) -> str:
        """
        Topic where the source connector publishes.

        - With `topic` set and no enricher: the source reroutes raw CDC events
          into `topic`, which is the public destination.
        - With an enricher: the source publishes to the raw Debezium topic;
          the enricher reads from there and produces to the public `topic`.
        """
        if cls.topic and not cls.has_enrich():
            return cls.topic
        return cls._raw_topic()

    @classmethod
    def get_enriched_topic(cls) -> str:
        """
        Final public topic. Used by the enricher's producer and by sinks
        consuming the public stream.
        """
        if cls.enriched_topic:
            return cls.enriched_topic
        if cls.topic:
            return cls.topic
        return cls._raw_topic()

    @classmethod
    def _raw_topic(cls) -> str:
        prefix = settings.MODEL_SYNC_TOPIC_PREFIX
        db_table = f"{settings.MODEL_SYNC_DB_SCHEMA}.{cls.model._meta.db_table}"
        return f"{prefix}.{db_table}" if prefix else db_table

class ModelSyncSinkConsumer(Consumer):
    """
    Default Consumer that runs the sink topics for ModelSyncs with PythonSink.

    Topics are auto-discovered from the model_sync registry via the
    owner-aware `Topics` descriptor. Used unless the project sets
    `MODEL_SYNC_CONSUMER` to its own consumer.
    """

    topics = Topics()
    config = {
        "group.id": settings.MODEL_SYNC_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
    }
