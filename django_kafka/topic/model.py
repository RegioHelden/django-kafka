import contextlib
from abc import ABC, abstractmethod

from confluent_kafka.serialization import MessageField
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Model

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import TopicConsumer


class ModelTopicConsumer(TopicConsumer, ABC):
    """Syncs abstract kafka messages directly in to Django model instances"""

    model: type[Model] | None = None  # override get_model for dynamic model lookups
    exclude_fields: list[str] = None  # fields to ignore from message value

    def transform(self, model, value) -> dict:
        """
        Filters key fields and runs defined `transform_{key}` methods.

        Value fields can be transformed by defining a `transform_{key}` method.
        """
        exclude_fields = self.exclude_fields or []

        transformed_value = {}
        for field_name, field_value in value.items():
            if field_name in exclude_fields:
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
        defaults = {
            field_name: field_value
            for field_name, field_value in value.items()
            if self.model_has_field(model, field_name)
        }

        if issubclass(model, KafkaConnectSkipModel):
            defaults["kafka_skip"] = True

        return defaults

    @abstractmethod
    def is_deletion(self, model, key, value) -> bool:
        """returns if the message represents a deletion"""

    @abstractmethod
    def get_lookup_kwargs(self, model, key, value) -> dict:
        """returns the lookup kwargs used for filtering the model instance"""

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
