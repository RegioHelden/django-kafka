import contextlib
from abc import ABC

from confluent_kafka.serialization import MessageField
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Model

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import TopicConsumer


class ModelTopicConsumer(TopicConsumer, ABC):
    """Syncs abstract kafka messages directly in to Django model instances"""

    model: type[Model] | None = None  # override get_model for dynamic model lookups
    exclude_fields: list[str] | None = None  # fields to ignore from message value
    deletion_key: str | None = "__deleted"  # value field that flags deletion
    lookup_field: str | None = "pk"  # "pk" for the model pk, None for the whole key

    def __init__(self, **kwargs):
        # local override of class values
        for field in ("deletion_key", "lookup_field"):
            if field in kwargs:
                setattr(self, field, kwargs.pop(field))

        super().__init__(**kwargs)

    def transform(self, model, value) -> dict:
        """
        Filters key fields and runs defined `transform_{key}` methods.

        Value fields can be transformed by defining a `transform_{key}` method.
        """
        transformed_value = {}
        for field_name, field_value in value.items():
            if self._is_field_excluded(field_name):
                continue
            if transform_method := getattr(self, f"transform_{field_name}", None):
                new_key, new_value = transform_method(model, field_name, field_value)
                transformed_value[new_key] = new_value
            else:
                transformed_value[field_name] = field_value
        return transformed_value

    def get_defaults(self, model, value) -> dict:
        """
        Returns instance update_or_create defaults from the message value.
        """
        return {
            field_name: field_value
            for field_name, field_value in value.items()
            if self.model_has_field(model, field_name)
        }

    def is_deletion(self, model, key, value) -> bool:
        """
        Returns whether the message represents a deletion.

        A `None` value is always treated as a tombstone. When `deletion_key`
        is set, that field in the value is also checked for a truthy boolean
        or `"true"` string marker. Override for non-standard schemes.
        """
        if value is None:
            return True

        if self.deletion_key:
            deleted = value.pop(self.deletion_key, None)
            if isinstance(deleted, bool):
                return deleted
            if isinstance(deleted, str):
                return deleted.lower() == "true"

        return False

    def _is_field_excluded(self, field):
        return self.exclude_fields and field in self.exclude_fields

    def get_lookup_field(self, model) -> str | None:
        """returns the field identifying the model instance, None for the whole key"""
        if self.lookup_field == "pk":
            return model._meta.pk.name
        return self.lookup_field

    def get_lookup_kwargs(self, model, key, value) -> dict:
        """returns the lookup kwargs used for filtering the model instance"""
        lookup_field = self.get_lookup_field(model)
        if lookup_field is None:
            return key  # whole key has been indicated

        if self._is_field_excluded(lookup_field):
            raise DjangoKafkaError(f"lookup_field {lookup_field} is excluded")

        # grab field from the value first so the key can be adjusted for partitioning
        if value and lookup_field in value:
            return {lookup_field: value[lookup_field]}
        if key and lookup_field in key:
            return {lookup_field: key[lookup_field]}

        raise DjangoKafkaError(f"lookup_field {lookup_field} missing from message")

    def sync(self, model, key, value) -> tuple[Model, bool] | None:
        lookup = self.get_lookup_kwargs(model, key, value)

        if self.is_deletion(model, key, value):
            with contextlib.suppress(ObjectDoesNotExist):
                model.objects.get(**lookup).delete()
            return None

        transformed_value = self.transform(model, value)
        defaults = self.get_defaults(model, transformed_value)
        return model.objects.update_or_create(**lookup, defaults=defaults)

    def get_model(self, key, value) -> type[Model]:
        if self.model:
            return self.model
        raise DjangoKafkaError(
            "Cannot obtain model: either define a default model or override get_model",
        )

    def model_has_field(self, model: type[Model], field_name: str) -> bool:
        return (
            field_name in model._meta._forward_fields_map
            or field_name in model._meta.fields_map
        )

    def consume(self, msg):
        key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        model = self.get_model(key, value)
        self.sync(model, key, value)
