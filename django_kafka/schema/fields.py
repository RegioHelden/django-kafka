import types
import typing

from django.db import models

DJANGO_FIELD_TO_AVRO = {
    models.AutoField: "int",
    models.SmallAutoField: "int",
    models.BigAutoField: "long",
    models.SmallIntegerField: "int",
    models.IntegerField: "int",
    models.BigIntegerField: "long",
    models.PositiveSmallIntegerField: "int",
    models.PositiveIntegerField: "int",
    models.PositiveBigIntegerField: "long",
    models.FloatField: "double",
    models.BooleanField: "boolean",
    models.CharField: "string",
    models.TextField: "string",
    models.SlugField: "string",
    models.EmailField: "string",
    models.URLField: "string",
    models.FilePathField: "string",
    models.GenericIPAddressField: "string",
    models.FileField: "string",
    models.UUIDField: {"type": "string", "logicalType": "uuid"},
    models.DateField: {"type": "int", "logicalType": "date"},
    models.DateTimeField: {"type": "long", "logicalType": "timestamp-millis"},
    models.TimeField: {"type": "int", "logicalType": "time-millis"},
    models.DurationField: "long",
    models.BinaryField: "bytes",
    models.JSONField: "string",
    models.DecimalField: lambda field: {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": field.max_digits,
        "scale": field.decimal_places,
    },
}

PYTHON_TYPE_TO_AVRO = {
    str: "string",
    int: "int",
    float: "double",
    bool: "boolean",
    bytes: "bytes",
}


def django_field_to_avro(field):
    """Convert a Django model field to its Avro type representation."""
    # Retrieve the type of the pk field of the related model
    if isinstance(field, (models.ForeignKey, models.OneToOneField)):
        return django_field_to_avro(field.related_model._meta.pk)

    # Iterate over the class definitions to find the mapped class.
    # (e.g. EmailField -> CharField -> "string")
    for cls in type(field).__mro__:
        if avro_type := DJANGO_FIELD_TO_AVRO.get(cls):
            if callable(avro_type):
                return avro_type(field)
            return avro_type

    raise ValueError(
        f"Unsupported Django field type: {type(field).__name__} "
        f"(field: {field.name})",
    )


def python_type_to_avro(python_type):
    """Convert a Python type annotation to its Avro type representation."""
    origin = getattr(python_type, "__origin__", None)
    args = getattr(python_type, "__args__", None)

    # Avro supports unions natively and we support only simple T|None -> e.g. ["null", "str"]
    is_union = (
        isinstance(python_type, types.UnionType)
        or origin is typing.Union
    )
    if is_union:
        # Strip None from the union members. For int | None → [int]
        non_none = [a for a in args if a is not type(None)]
        # Only handle the simple T | None case (one real type + None)
        if len(non_none) == 1:
            avro_type = python_type_to_avro(non_none[0])
            # Convert the single non-None type and wrap in an Avro nullable union:
            return ["null", avro_type]
        # Could theoretically extend more complex types, but not necessary r/n
        raise ValueError(f"Unsupported union type: {python_type}")

    if not (avro_type := PYTHON_TYPE_TO_AVRO.get(python_type)):
        raise ValueError(f"Unsupported Python type: {python_type}")
    return avro_type
