import datetime
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Flag, auto
from typing import TYPE_CHECKING, Any, get_type_hints

from django.db.models import Model

from django_kafka.schema.avro import AvroSchema
from django_kafka.schema.fields import python_type_to_avro

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


class MessagePart(Flag):
    KEY = auto()
    VALUE = auto()
    BOTH = KEY | VALUE


@dataclass
class Transform(ABC):
    """
    Base class for message transformations.

    Every Transform receives the full `(msg_key, msg_value)` and returns the
    updated pair. Subclasses choose which side(s) to mutate based on their
    own configuration.

    Schema derivation is symmetric: subclasses return updated `(key_fields,
    value_fields)` lists. The framework calls these once per message; the
    transform decides per-side what (if anything) to do.
    """

    @abstractmethod
    def apply(
        self,
        sync: "ModelSync",
        msg_key: dict,
        msg_value: dict,
    ) -> tuple[dict, dict]:
        """Return new `(msg_key, msg_value)`."""

    @abstractmethod
    def update_schema(
        self,
        sync: "ModelSync",
        key_fields: list[dict],
        value_fields: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Return updated `(key_fields, value_fields)`."""

    def produces(self, sync: "ModelSync") -> set[str]:
        """
        Field names this transform writes into the message.

        The sink uses this (together with `IncludeFields` / `ExcludeFields`)
        to decide what may be persisted: a field that no transform produces
        and isn't allowed by `fields` is dropped before `update_or_create`.

        Default: nothing. FieldTransform / EnricherTransform override.
        """
        return set()


@dataclass
class FieldTransform(Transform, ABC):
    """
    Base for per-field transformations (rename / coerce / lookup a single
    field). Most transforms are this shape.

    `source`: field name in the incoming message.
    `target`: field name to write to (defaults to `source`).
    `apply_to`: which part(s) of the message this transform runs on.
    `replace`: when True, source is removed; when False, source stays
        alongside target.

    Subclasses implement:
      - `transform_value(sync, msg_key, msg_value, part)`:
        compute the new value for the field. `part` indicates which side
        will receive the result.
      - `output_avro_type(sync, schema_field)`: Avro type of the
        produced field. Same type used for both sides when `apply_to=BOTH`.
    """

    source: str
    target: str | None = None
    apply_to: MessagePart = MessagePart.VALUE
    replace: bool = True

    @abstractmethod
    def transform_value(
        self,
        sync: "ModelSync",
        msg_key: dict,
        msg_value: dict,
        part: MessagePart,
    ) -> Any:
        """Compute the new value for `source`."""

    def output_avro_type(
        self,
        sync: "ModelSync",
        schema_field: dict | None,
    ) -> Any:
        """
        Avro type of the produced field.
        Default: keep the source field's type.
        Subclasses override when the produced type differs from source.
        """
        if schema_field is None:
            raise ValueError(
                f"{type(self).__name__}.output_avro_type cannot fall back to "
                f"the source field type because '{self.source}' is not in the schema.",
            )
        return schema_field["type"]

    def apply(self, sync, msg_key, msg_value):
        # Compute new values from the *original* key+value so each side's
        # `transform_value` sees the same input regardless of order.
        new_key = msg_key
        new_value = msg_value
        if self.apply_to & MessagePart.KEY:
            new_key = self._apply_to_part(sync, msg_key, msg_value, MessagePart.KEY)
        if self.apply_to & MessagePart.VALUE:
            new_value = self._apply_to_part(sync, msg_key, msg_value, MessagePart.VALUE)
        return new_key, new_value

    def _apply_to_part(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        result = dict(message)
        if self.replace:
            result.pop(self.source, None)
        result[self.target or self.source] = self.transform_value(
            sync,
            msg_key,
            msg_value,
            part,
        )
        return result

    def update_schema(self, sync, key_fields, value_fields):
        if self.apply_to & MessagePart.KEY:
            key_fields = self._update_part_schema(sync, key_fields)
        if self.apply_to & MessagePart.VALUE:
            value_fields = self._update_part_schema(sync, value_fields)
        return key_fields, value_fields

    def produces(self, sync):
        return {self.target or self.source}

    def _update_part_schema(self, sync, fields):
        target = self.target or self.source
        schema_field = next(
            (f for f in fields if f["name"] == self.source),
            None,
        )
        result = [
            f
            for f in fields
            # keep source unless we're replacing it; drop existing target
            if (f["name"] != self.source or not self.replace) and f["name"] != target
        ]
        result.append(
            {
                "name": target,
                "type": self.output_avro_type(sync, schema_field),
            },
        )
        return result


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

    Replaces the message field with `model.objects.get(<id_field>=value)`,
    assigned to `target` (the FK attribute on the consuming model).
    A null (or absent) message value assigns `None` instead of a lookup.
    """

    model: type[Model] | None = None
    id_field: str = ""

    def transform_value(self, sync, msg_key, msg_value, part):
        message = msg_key if part == MessagePart.KEY else msg_value
        id_value = message.get(self.source)
        if id_value is None:
            return None
        return self.model.objects.get(**{self.id_field: id_value})


@dataclass
class EnricherTransform(Transform):
    """
    Adds derived fields to the message by calling a method on the
    ModelSync.

    The method receives `(msg_key, msg_value)` and returns extras to merge
    into the message; its return type annotation (typically a TypedDict)
    drives the Avro schema delta.

    `method`: name of the sync method to call. Defaults to "enrich".
    `apply_to`: which side(s) get the new fields. Defaults to VALUE since
        adding computed fields to the key changes message identity.

    Subclass and override `enrich` / `output_type` to compute extras
    without delegating to a sync method.
    """

    method: str = "enrich"
    apply_to: MessagePart = MessagePart.VALUE

    def enrich(
        self,
        sync: "ModelSync",
        msg_key: dict,
        msg_value: dict,
    ) -> dict:
        """Return new fields to merge into the message."""
        return self._resolve_method(sync)(msg_key, msg_value)

    def output_type(self, sync: "ModelSync") -> type:
        """Return a typed class describing the fields added by `enrich`."""
        method = self._resolve_method(sync)
        return_type = get_type_hints(method).get("return")
        if return_type is None:
            raise TypeError(
                f"{getattr(method, '__qualname__', method)} must declare a "
                f"return type annotation for schema derivation.",
            )
        return return_type

    def _resolve_method(self, sync) -> Callable[[dict, dict], dict]:
        return getattr(sync, self.method)

    def apply(self, sync, msg_key, msg_value):
        extras = self.enrich(sync, msg_key, msg_value)
        if self.apply_to & MessagePart.KEY:
            msg_key = {**msg_key, **extras}
        if self.apply_to & MessagePart.VALUE:
            msg_value = {**msg_value, **extras}
        return msg_key, msg_value

    def update_schema(self, sync, key_fields, value_fields):
        added = AvroSchema.from_type(self.output_type(sync)).fields
        if self.apply_to & MessagePart.KEY:
            key_fields = self._merge_fields(key_fields, added)
        if self.apply_to & MessagePart.VALUE:
            value_fields = self._merge_fields(value_fields, added)
        return key_fields, value_fields

    def produces(self, sync):
        return set(get_type_hints(self.output_type(sync)).keys())

    @staticmethod
    def _merge_fields(existing: list[dict], added: list[dict]) -> list[dict]:
        names = {f["name"] for f in existing}
        return [*existing, *[f for f in added if f["name"] not in names]]


class TopicTransformsMixin:
    """
    Resolves and applies a list of Transforms against a ModelSync.
    """

    def __init__(
        self,
        *args,
        transforms: list[Transform] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.transforms: list[Transform] = transforms or []

    def apply_transforms(
        self,
        sync: "ModelSync",
        msg_key: dict | None,
        msg_value: dict | None,
    ) -> tuple[dict | None, dict | None]:
        """Apply transforms in declared order, returning the final pair."""
        for transform in self.transforms:
            msg_key, msg_value = transform.apply(sync, msg_key or {}, msg_value or {})
        return msg_key, msg_value

    def update_schema(
        self,
        sync: "ModelSync",
        key_fields: list[dict],
        value_fields: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Walk transforms to derive the post-transform Avro schemas."""
        for transform in self.transforms:
            key_fields, value_fields = transform.update_schema(
                sync,
                key_fields,
                value_fields,
            )
        return key_fields, value_fields
