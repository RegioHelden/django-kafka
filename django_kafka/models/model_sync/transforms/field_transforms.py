from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, get_type_hints

from django.db.models import ForeignKey, Model

from django_kafka.schema.fields import python_type_to_avro

from .base import FieldTransform
from .utils import MessagePart

if TYPE_CHECKING:
    from collections.abc import Callable


_missing = object()


@dataclass
class CoalesceTransform(FieldTransform):
    """Replace `None` with `default`, otherwise keep the value as-is."""

    default: Any = None

    def transform_value(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        v = message.get(self.source)
        return v if v is not None else self.default


@dataclass
class StaticValueTransform(FieldTransform):
    """Always set the field to `value`, ignoring the incoming value."""

    value: Any = None

    def transform_value(self, sync, msg_key, msg_value, part):
        return self.value

    def output_avro_type(self, sync, schema_field):
        return python_type_to_avro(type(self.value))


@dataclass
class DateFromEpochTransform(FieldTransform):
    """
    Convert an Avro `int` (`logicalType: date`) into a `datetime.date`.

    Confluent's default AvroDeserializer doesn't auto-convert logical types,
    so date fields arrive as days-since-epoch ints. Use this when consuming
    a topic produced by Debezium's PostgreSQL connector.
    """

    epoch_date = datetime.date(1970, 1, 1)

    def transform_value(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        days = message.get(self.source)
        if days is None or days == "":
            return None
        return self.epoch_date + datetime.timedelta(days=days)

    def output_avro_type(self, sync, schema_field):
        # The wire type stays int — only the Python representation changes.
        return schema_field["type"] if schema_field else "int"


@dataclass
class DateTimeFromEpochMillisTransform(FieldTransform):
    """
    Convert an Avro `long` (`logicalType: timestamp-millis`) into a
    timezone-aware `datetime.datetime`.
    """

    def transform_value(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        millis = message.get(self.source)
        if millis is None or millis == "":
            return None
        return datetime.datetime.fromtimestamp(
            millis / 1000,
            tz=datetime.UTC,
        )

    def output_avro_type(self, sync, schema_field):
        return schema_field["type"] if schema_field else "long"


@dataclass
class SyncMethodTransform(FieldTransform):
    """
    Delegate to a method on the ModelSync.

    If `method` is set, calls `getattr(sync, method)(msg_key, msg_value)`.
    Otherwise falls back to `getattr(sync, f"{prefix}_{source}")(msg_key, msg_value)`.

    Use `EnrichMethodTransform` or `ConsumeMethodTransform` for auto-naming
    without an explicit `method`.

    The user method receives `(msg_key, msg_value)` and returns the new field
    value. Same value used for both sides when `apply_to=BOTH`.

    The method's return type annotation drives the Avro schema delta.
    """

    method: str | None = None
    prefix: str = ""

    def _resolve_method(self, sync) -> Callable[[dict, dict], Any]:
        return getattr(sync, self.method or f"{self.prefix}_{self.source}")

    def transform_value(self, sync, msg_key, msg_value, part):
        return self._resolve_method(sync)(msg_key, msg_value)

    def output_avro_type(self, sync, schema_field):
        method = self._resolve_method(sync)
        return_type = get_type_hints(method).get("return")
        if return_type is None:
            raise TypeError(
                f"{getattr(method, '__qualname__', method)} must declare a "
                f"return type annotation for schema derivation.",
            )
        return python_type_to_avro(return_type)


@dataclass
class EnrichMethodTransform(SyncMethodTransform):
    """Calls `enrich_<source>` on the ModelSync (or explicit `method`)."""

    prefix: str = "enrich"


@dataclass
class ConsumeMethodTransform(SyncMethodTransform):
    """Calls `consume_<source>` on the ModelSync (or explicit `method`)."""

    prefix: str = "consume"


@dataclass
class RelationTransform(FieldTransform):
    """
    Field transform that resolves a foreign-key relation.

    Either form makes the relations resolver hold the message until the
    related row exists; `target` decides what happens to the value once it
    does.

    Without `target` the message is left untouched. A plain FK already
    arrives under the model's own column (`customer_id: 7`), so it is written
    as it stands and no query is made - the transform only marks where the
    relation resolves. This is what auto-detection emits.

        {"customer_id": 7, "amount": 5} -> unchanged

    With `target` the id is swapped for the instance: `model.objects.get(
    <id_field>=value)` assigned to `target`, and `source` dropped unless
    `replace=False`. Needed when
    the message carries something the FK column cannot take - a uuid, or a
    field renamed by an enrich transform. A null (or absent) value assigns
    `None` without a lookup.

        {"customer__uuid": "ab-12"} -> {"customer": <Customer ab-12>}

    Position in `consume_transforms` decides when the relation resolves:
    every step before it has already run, and every step after it can count
    on the row existing - on the instance too, where `target` is set.

    `lookup`: model lookup path replacing `source` when the sink looks up the
    row being synced. Set it when the message field name doesn't match the
    path on the model (e.g. `user__kafka_uuid` -> `customer_user__kafka_uuid`).
    """

    model: type[Model] | None = None
    id_field: str = ""
    lookup: str | None = None

    def resolves(self, field) -> bool:
        """Whether this transform resolves `field`, a foreign key on the model."""
        if not isinstance(field, ForeignKey):
            return False
        if self.target:
            return self.target == field.name
        # writing nothing, the id can only reach the model under the fk's column
        return self.source == field.attname

    def apply(self, sync, msg_key, msg_value):
        if self.target is None:
            # super() would add an absent id back as None, nulling the fk
            return msg_key, msg_value
        return super().apply(sync, msg_key, msg_value)

    def transform_value(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        id_value = message.get(self.source)
        if id_value is None:
            return None
        return self.model.objects.get(**{self.id_field: id_value})
