import inspect
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING

from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    Serializer,
    StringDeserializer,
    StringSerializer,
)

from django_kafka import kafka
from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.producer import Suppression

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.relations_resolver.relation import Relation
    from django_kafka.retry.settings import RetrySettings

logger = logging.getLogger(__name__)


class TopicProducer(ABC):
    key_serializer: type[Serializer] = StringSerializer
    value_serializer: type[Serializer] = StringSerializer

    @property
    @abstractmethod
    def name(self) -> str:
        """Define Kafka topic producing topic name"""

    def __init__(self):
        super().__init__()
        if self.name.startswith("^"):
            raise DjangoKafkaError(f"TopicProducer name cannot be regex: {self.name}")

    def get_key_serializer(self, **kwargs):
        return self.key_serializer(**kwargs)

    def get_value_serializer(self, **kwargs):
        return self.value_serializer(**kwargs)

    def context(
        self,
        field: MessageField,
        headers: list | None = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)

    def serialize(
        self,
        value,
        field: MessageField,
        headers: list | None = None,
        **kwargs,
    ):
        if field == MessageField.VALUE:
            serializer = self.get_value_serializer(**kwargs)
            return serializer(value, self.context(MessageField.VALUE, headers))

        if field == MessageField.KEY:
            serializer = self.get_key_serializer(**kwargs)
            return serializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported serialization field {field}.")

    def produce(self, value: any, **kwargs):
        if Suppression.active(self.name):
            return  # do not serialize if producing is suppressed

        key_serializer_kwargs = kwargs.pop("key_serializer_kwargs", {}) or {}
        value_serializer_kwargs = kwargs.pop("value_serializer_kwargs", {}) or {}
        headers = kwargs.get("headers")

        if "key" in kwargs:
            kwargs["key"] = self.serialize(
                kwargs["key"],
                MessageField.KEY,
                headers,
                **key_serializer_kwargs,
            )

        kafka.producer.produce(
            self.name,
            self.serialize(
                value,
                MessageField.VALUE,
                headers,
                **value_serializer_kwargs,
            ),
            **kwargs,
        )
        kafka.producer.poll(0)  # stops producer on_delivery callbacks buffer overflow


class TopicConsumer(ABC):
    key_deserializer: type[Deserializer] = StringDeserializer
    value_deserializer: type[Deserializer] = StringDeserializer
    retry_settings: "RetrySettings" = settings.get_retry_settings()

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Defines Kafka topic consumer subscription name.

        Regexp pattern subscriptions are supported by prefixing the name with "^".
        """

    def is_regex(self) -> bool:
        """Returns if the subscription name is regex based"""
        return self.name.startswith("^")

    def matches(self, topic_name: str) -> bool:
        if self.is_regex():
            return bool(re.search(self.name, topic_name))
        return self.name == topic_name

    def get_key_deserializer(self, **kwargs):
        return self.key_deserializer(**kwargs)

    def get_value_deserializer(self, **kwargs):
        return self.value_deserializer(**kwargs)

    def context(
        self,
        field: MessageField,
        headers: list | None = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)

    def deserialize(
        self,
        value,
        field: MessageField,
        headers: list | None = None,
        **kwargs,
    ):
        if field == MessageField.VALUE:
            deserializer = self.get_value_deserializer(**kwargs)
            return deserializer(value, self.context(MessageField.VALUE, headers))

        if field == MessageField.KEY:
            deserializer = self.get_key_deserializer(**kwargs)
            return deserializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported deserialization field {field}.")

    def consume(self, msg: "cimpl.Message"):
        """Implement message processing"""
        raise NotImplementedError

    def get_relations(self, msg: "cimpl.Message") -> Iterator["Relation"]:
        """
        Dependency resolver will kick-in in case this method yields
        yield ModelRelation(...)
        """
        yield from []

    @property
    def use_relations_resolver(self) -> bool:
        base_method = inspect.getattr_static(TopicConsumer, "get_relations", None)
        obj_method = inspect.getattr_static(type(self), "get_relations", None)
        return obj_method is not base_method


class Topic(TopicConsumer, TopicProducer, ABC):
    """Combined Topic class for when topic consume and produce names are the same"""
