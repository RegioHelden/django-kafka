from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.models.model_sync.fields import ExcludeFields
from django_kafka.models.model_sync.sink.base import ConnectorSink

if TYPE_CHECKING:
    from django_kafka.models.model_sync.fields import IncludeFields
    from django_kafka.models.model_sync.sync import ModelSync


class DbzJdbcSink(ConnectorSink):
    """
    Kafka Connect sink that writes consumed messages back into the model's
    table via Debezium's JDBC sink connector.

    On registration, the framework wraps this sink in a standalone Connector
    (see `ModelSyncRegistry._register_connector_sink`). The generated config
    is submitted to the Kafka Connect REST API.

    Defaults configure the typical CDC-style sync:
        - `insert_mode="upsert"` — INSERT ON CONFLICT UPDATE on the PK
        - `delete_enabled=True` — tombstones (value=None) become DELETEs
        - `pk_mode="record_key"` — PK comes from the message key
        - `pk_fields` — defaults to the model's PK column
        - `schema.evolution="basic"` — auto-add missing columns when encountered
        - `driver="postgresql"` — JDBC URL scheme (e.g. "mysql", "sqlserver")

    For bidirectional syncs (model also has a `source`), the framework adds
    transforms that drop the incoming `kafka_skip` and re-add it as `True`
    so that DB-applied changes are flagged and the source-side filter drops
    them - preventing CDC loops.
    """

    def __init__(
        self,
        instance: "ModelSync | None" = None,
        topics: list[str] | str | None = None,
        insert_mode: str = "upsert",
        delete_enabled: bool = True,
        pk_mode: str = "record_key",
        pk_fields: list[str] | None = None,
        fields: "IncludeFields | ExcludeFields | None" = None,
        database: dict | None = None,
        driver: str = "postgresql",
    ):
        super().__init__(
            instance=instance,
            topics=topics,
            insert_mode=insert_mode,
            delete_enabled=delete_enabled,
            pk_mode=pk_mode,
            pk_fields=pk_fields,
            fields=fields,
            database=database,
            driver=driver,
        )
        self._topics = [topics] if isinstance(topics, str) else topics
        self.insert_mode = insert_mode
        self.delete_enabled = delete_enabled
        self.pk_mode = pk_mode
        self.pk_fields = pk_fields
        self._fields = fields
        self._database = database
        self.driver = driver

    @property
    def topics(self) -> list[str]:
        # Subscribe to the enriched topic when the sync runs an enricher,
        # otherwise the raw source topic. Explicit `topics=` takes precedence.
        if self._topics is not None:
            return self._topics
        if self.instance.has_enrich():
            return [self.instance.get_enriched_topic()]
        return [self.instance.source_topic()]

    @property
    def db(self) -> dict:
        return self._database or settings.CONNECT_DATABASE

    @property
    def db_table(self) -> str:
        return f"public.{self.instance.model._meta.db_table}"

    @property
    def _connection_url(self) -> str:
        url = f"jdbc:{self.driver}://{self.db['HOST']}"
        if self.db.get("PORT"):
            url = f"{url}:{self.db['PORT']}"
        return f"{url}/{self.db['NAME']}"

    @property
    def config(self) -> dict:
        pk_fields = self.pk_fields or [self.instance.model._meta.pk.column]
        config = {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "connection.url": self._connection_url,
            "connection.username": self.db["USER"],
            "connection.password": self.db["PASSWORD"],
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": settings.SCHEMA_REGISTRY["url"],
            "key.converter.enhanced.avro.schema.support": True,
            "key.converter.key.subject.name.strategy": (
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
            ),
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": settings.SCHEMA_REGISTRY["url"],
            "value.converter.enhanced.avro.schema.support": True,
            "value.converter.value.subject.name.strategy": (
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
            ),
            "topics": ",".join(self.topics),
            "table.name.format": self.db_table,
            "insert.mode": self.insert_mode,
            "delete.enabled": self.delete_enabled,
            "primary.key.mode": self.pk_mode,
            "primary.key.fields": ",".join(pk_fields),
            # `basic` lets Debezium auto-add missing columns at INSERT time.
            # Required for the kafka_skip column added by `_set_kafka_skip`.
            "schema.evolution": "basic",
        }
        self._set_fields(config)
        if self.instance.is_bidirectional():
            self._set_kafka_skip(config)
        return config

    def _set_fields(self, config: dict) -> None:
        # Translate IncludeFields/ExcludeFields into a ReplaceField$Value SMT.
        # Sink-side `fields` overrides the ModelSync's `fields`, letting
        # consumers choose a different column subset than the source emits.
        fields = self._fields or self.instance.fields
        if fields is None:
            return

        transform_name = f"Ms{fields.__class__.__name__}"
        op = "exclude" if isinstance(fields, ExcludeFields) else "include"
        config["transforms"] = transform_name
        config[f"transforms.{transform_name}.type"] = (
            "org.apache.kafka.connect.transforms.ReplaceField$Value"
        )
        config[f"transforms.{transform_name}.{op}"] = ",".join(fields)

    def _set_kafka_skip(self, config: dict) -> None:
        # CDC loop prevention for bidirectional syncs.
        # Drop the incoming kafka_skip, then re-add it as `true` so the row
        # written by this sink is flagged and the source-side filter ignores
        # the resulting CDC event. Cast normalizes InsertField's string value
        # to a boolean.
        rm = "MsRmKafkaSkip"
        add = "MsAddKafkaSkip"
        cast = "MsCastKafkaSkip"
        existing = config.get("transforms")
        new_transforms = f"{rm},{add},{cast}"
        if existing:
            config["transforms"] = f"{existing},{new_transforms}"
        else:
            config["transforms"] = new_transforms
        config[f"transforms.{rm}.type"] = (
            "org.apache.kafka.connect.transforms.ReplaceField$Value"
        )
        config[f"transforms.{rm}.exclude"] = "kafka_skip"
        config[f"transforms.{add}.type"] = (
            "org.apache.kafka.connect.transforms.InsertField$Value"
        )
        config[f"transforms.{add}.static.field"] = "kafka_skip"
        config[f"transforms.{add}.static.value"] = True
        config[f"transforms.{cast}.type"] = (
            "org.apache.kafka.connect.transforms.Cast$Value"
        )
        config[f"transforms.{cast}.spec"] = "kafka_skip:boolean"
