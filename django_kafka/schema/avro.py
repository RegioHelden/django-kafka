import json
import struct
from copy import deepcopy
from typing import get_type_hints

from django_kafka.schema.fields import (
    django_field_to_avro,
    python_type_to_avro,
)


def get_writer_schema(raw_bytes: bytes) -> str:
    """
    Extract the writer schema for an Avro-encoded payload from the registry.

    Confluent's `AvroDeserializer` resolves the schema internally but does not
    expose it on the deserialized object. We need the writer schema to derive
    the enriched topic's schema, so we parse the Confluent wire format
    ourselves: byte 0 is the magic byte, bytes 1-4 are the big-endian schema
    id, then we look the schema up in the registry.

    See: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    """
    from django_kafka import kafka  # noqa: PLC0415 — avoid circular import

    schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
    return kafka.schema_client.get_schema(schema_id).schema_str


class AvroSchema:
    """
    Avro schema builder with composable API.

    Usage:
        # From a Django model
        schema = AvroSchema.from_model(MyModel, fields=("id", "name"))
        schema = AvroSchema.from_model(MyModel, fields=("password",), exclude=True)

        # From type annotations (e.g. TypedDict)
        schema = AvroSchema.from_type(ExtraFields)

        # Key schema from model lookup fields
        schema = AvroSchema.key_from_model(MyModel, ("id",))

        # Compose schemas
        schema = AvroSchema.from_model(MyModel).extend(ExtraFields)

        # Output
        schema.to_json()   # JSON string
        schema.to_dict()   # dict
    """

    def __init__(self, name, fields, namespace=None):
        self.name = name
        self.namespace = namespace
        self.fields = list(fields)

    @staticmethod
    def _get_model_fields(model, fields=None, exclude=False):
        """Return concrete model fields filtered by `fields`.

        If `exclude=True`, returns all fields except those in `fields`.
        If `fields` is None, returns all concrete fields.
        """
        all_fields = {f.column: f for f in model._meta.fields}

        if not fields:
            return all_fields

        if not exclude:
            # Safeguard to not accidentally define something which does not exist.
            missing = set(fields) - all_fields.keys()
            if missing:
                raise ValueError(
                    f"Fields not found on {model.__name__}: {sorted(missing)}. "
                    f"Available: {list(all_fields)}",
                )

        # If exclude=True and the name is found (which is also True), the field won't be included
        # and other way around, if exclude=False and field found (which is True) - field included
        return {
            name: f for name, f in all_fields.items()
            if (name in fields) != exclude
        }

    @classmethod
    def from_model(
        cls, model, fields=None, exclude=False, namespace=None, name=None,
    ):
        """
        Generate Avro schema from a Django model.

        Args:
            model: Django model class
            fields: List of field names to include (or exclude if `exclude=True`)
            exclude: When True, treat `fields` as an exclusion list
            namespace: Avro schema namespace
            name: Avro record name (defaults to model class name)
        """
        model_fields = cls._get_model_fields(model, fields, exclude)
        avro_fields = []

        for field_name, field in model_fields.items():
            avro_type = django_field_to_avro(field)

            has_default = field.has_default()
            nullable = getattr(field, "null", False)

            if nullable:
                avro_type = ["null", avro_type]
                if not has_default:
                    avro_fields.append(
                        {"name": field_name, "type": avro_type, "default": None},
                    )
                    continue

            avro_fields.append({"name": field_name, "type": avro_type})

        return cls(
            name=name or model.__name__,
            fields=avro_fields,
            namespace=namespace,
        )

    @classmethod
    def from_json(cls, schema_str: str) -> "AvroSchema":
        """Parse a serialized Avro schema JSON string into an AvroSchema."""
        schema = json.loads(schema_str)
        return cls(
            name=schema["name"],
            fields=schema["fields"],
            namespace=schema.get("namespace"),
        )

    @classmethod
    def from_type(cls, annotation_type, namespace=None, name=None):
        """
        Generate Avro schema from a type's annotations.

        Args:
            annotation_type: A class with __annotations__ (TypedDict, dataclass, etc.)
            namespace: Avro schema namespace
            name: Avro record name
        """
        hints = get_type_hints(annotation_type)
        avro_fields = []

        for field_name, python_type in hints.items():
            avro_type = python_type_to_avro(python_type)
            avro_fields.append({"name": field_name, "type": avro_type})

        return cls(
            name=name or getattr(annotation_type, "__name__", "Extra"),
            fields=avro_fields,
            namespace=namespace,
        )

    @classmethod
    def key_from_model(cls, model, lookup_fields, namespace=None):
        """
        Generate Avro key schema from lookup field(s).

        Args:
            model: Django model class
            lookup_fields: Single field name or tuple of field names
            namespace: Avro schema namespace
        """
        if isinstance(lookup_fields, str):
            lookup_fields = (lookup_fields,)

        all_fields = cls._get_model_fields(model)
        avro_fields = []

        for field_name in lookup_fields:
            if field_name == "pk":
                field_name = model._meta.pk.attname

            if field_name not in all_fields:
                raise ValueError(
                    f"Lookup field '{field_name}' not found on model {model.__name__}.",
                )

            field = all_fields[field_name]
            avro_type = django_field_to_avro(field)
            avro_fields.append({"name": field_name, "type": avro_type})

        return cls(name="Key", fields=avro_fields, namespace=namespace)

    def extend(self, annotation_type):
        """
        Extend this schema with extra fields from a type's annotations.

        Returns a new AvroSchema with the extra fields appended.
        """
        extra = AvroSchema.from_type(annotation_type)
        return AvroSchema(
            name=self.name,
            fields=self.fields + extra.fields,
            namespace=self.namespace,
        )

    def to_dict(self):
        """Return schema as a dict."""
        # deepcopy so callers can mutate the returned dict without corrupting
        # this AvroSchema instance's fields.
        schema = {
            "type": "record",
            "name": self.name,
            "fields": deepcopy(self.fields),
        }
        if self.namespace:
            schema["namespace"] = self.namespace
        return schema

    def to_json(self):
        """Return schema as a JSON string."""
        return json.dumps(self.to_dict())
