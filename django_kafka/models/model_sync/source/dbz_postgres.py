from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.models.model_sync.fields import ExcludeFields
from django_kafka.models.model_sync.source.base import ConnectorSource

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


class DbzPostgresSource(ConnectorSource):
    def __init__(
        self,
        instance: "ModelSync | None" = None,
        connector: str | None = None,
        msg_key_fields: list[str] | None = None,
        replica_identity: str = "FULL",
    ):
        super().__init__(
            instance=instance,
            connector=connector,
            msg_key_fields=msg_key_fields,
            replica_identity=replica_identity,
        )
        self.connector: str | None = connector
        self.msg_key_fields: list[str] = msg_key_fields or ["pk"]
        self.replica_identity: str = replica_identity

    @property
    def db_table(self) -> str:
        return f"{settings.MODEL_SYNC_DB_SCHEMA}.{self.instance.model._meta.db_table}"

    def setup(self, config: dict) -> None:
        self._set_table(config)
        self._set_replica_identity(config)
        self._set_msg_key_fields(config)
        self._set_fields(config)
        if self.instance.is_bidirectional():
            self._set_kafka_skip_filter(config)
            self._set_rm_kafka_skip(config)
        # Reroute when an explicit topic is set AND there's no enricher in
        # the pipeline. With an enricher, the source publishes to the default
        # raw topic and the enricher takes care of producing to `topic`.
        if self.instance.topic and not self.instance.has_enrich():
            self._set_reroute(config)

    def _set_table(self, config: dict) -> None:
        param = "table.include.list"
        existing = config.get(param, "")
        tables = [t for t in existing.split(",") if t] if existing else []
        if self.db_table not in tables:
            tables.append(self.db_table)
        config[param] = ",".join(tables)

    def _set_replica_identity(self, config: dict) -> None:
        param = "replica.identity.autoset.values"
        existing = config.get(param, "")
        entries = [e for e in existing.split(",") if e] if existing else []
        new_entry = f"{self.db_table}:{self.replica_identity}"
        entries.append(new_entry)
        config[param] = ",".join(entries)

    def _set_msg_key_fields(self, config: dict) -> None:
        param = "message.key.columns"
        existing = config.get(param, "")
        entries = [e for e in existing.split(";") if e] if existing else []
        new_entry = f"{self.db_table}:{','.join(self.msg_key_fields)}"
        entries.append(new_entry)
        config[param] = ";".join(entries)

    def _set_kafka_skip_filter(self, config: dict) -> None:
        transform_name = f"MsKafkaSkipFilter{self.instance.__class__.__name__}"
        existing = config.get("transforms", "")
        transforms = [t for t in existing.split(",") if t] if existing else []
        if transform_name not in transforms:
            config["transforms"] = ",".join([transform_name, *transforms])
        raw_topic = f"{config['topic.prefix']}.{self.db_table}"
        config[f"transforms.{transform_name}.type"] = "io.debezium.transforms.Filter"
        config[f"transforms.{transform_name}.language"] = "jsr223.groovy"
        config[f"transforms.{transform_name}.topic.regex"] = raw_topic
        config[f"transforms.{transform_name}.condition"] = (
            "!valueSchema.field('before')?.schema()?.field('kafka_skip') "
            "|| (value.op == 'd' && !value.before?.kafka_skip) "
            "|| (value.op != 'd' && !value.after?.kafka_skip)"
        )

    def _set_rm_kafka_skip(self, config: dict) -> None:
        transform_name = "MsRmKafkaSkip"
        existing = config.get("transforms", "")
        # do not use set - sets are unordered
        transforms = [t for t in existing.split(",") if t] if existing else []
        if transform_name not in transforms:
            transforms.append(transform_name)
            config["transforms"] = ",".join(transforms)
        config[f"transforms.{transform_name}.type"] = (
            "org.apache.kafka.connect.transforms.ReplaceField$Value"
        )
        config[f"transforms.{transform_name}.exclude"] = "kafka_skip"

    def _set_reroute(self, config: dict) -> None:
        transform_name = f"MsReroute{self.instance.__class__.__name__}"
        existing = config.get("transforms", "")
        transforms = [t for t in existing.split(",") if t] if existing else []
        if transform_name not in transforms:
            transforms.append(transform_name)
            config["transforms"] = ",".join(transforms)
        raw_topic = f"{config['topic.prefix']}.{self.db_table}"
        config[f"transforms.{transform_name}.type"] = (
            "io.debezium.transforms.ByLogicalTableRouter"
        )
        config[f"transforms.{transform_name}.topic.regex"] = raw_topic
        config[f"transforms.{transform_name}.topic.replacement"] = self.instance.topic
        config[f"transforms.{transform_name}.key.enforce.uniqueness"] = False
        config[f"transforms.{transform_name}.schema.name.adjustment.mode"] = "avro"

    def _set_fields(self, config: dict) -> None:
        fields = self.instance.fields
        param = "column.include.list"
        existing = config.get(param, "")
        entries = [e for e in existing.split(",") if e] if existing else []

        if fields is None:
            entries.append(f"{self.db_table}.*")
        elif isinstance(fields, ExcludeFields):
            excluded = set(fields)
            # `kafka_skip` is required by the source-side loop filter on
            # bidirectional syncs; never drop it from the include list.
            if self.instance.is_bidirectional():
                excluded.discard("kafka_skip")
            entries.extend(
                f"{self.db_table}.{f.column}"
                for f in self.instance.model._meta.local_fields
                if f.column not in excluded
            )
        else:
            included = list(fields)
            # `kafka_skip` is required by the source-side loop filter on
            # bidirectional syncs; ensure it's present.
            if self.instance.is_bidirectional() and "kafka_skip" not in included:
                included.append("kafka_skip")
            entries.extend(f"{self.db_table}.{field}" for field in included)

        config[param] = ",".join(entries)
