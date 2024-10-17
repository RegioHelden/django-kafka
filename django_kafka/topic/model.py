import contextlib
from abc import ABC, abstractmethod
from typing import Optional, Type

from confluent_kafka.serialization import MessageField
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Model

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import TopicConsumer


class ModelTopicConsumer(TopicConsumer, ABC):
    """Syncs abstract kafka messages directly in to Django model instances"""

    model: Optional[Type[Model]] = None  # override get_model for dynamic model lookups

    def get_defaults(self, model, value) -> dict:
        """
        Returns instance update_or_create defaults from the message value.

        value fields can be transformed by defining a transform_{attr} method.
        """
        defaults = {}
        for attr, attr_value in value.items():
            if transform_method := getattr(self, "transform_" + attr, None):
                new_attr, new_value = transform_method(model, attr, attr_value)
                defaults[new_attr] = new_value
            else:
                defaults[attr] = attr_value

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

        defaults = self.get_defaults(model, value)
        return model.objects.update_or_create(**lookup, defaults=defaults)

    def get_model(self, key, value) -> Type[Model]:
        if self.model:
            return self.model
        raise DjangoKafkaError(
            "Cannot obtain model: either define a default model or override get_model",
        )

    def consume(self, msg):
        key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        model = self.get_model(key, value)
        self.sync(model, key, value)
