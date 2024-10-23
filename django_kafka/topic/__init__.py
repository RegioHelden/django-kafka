import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Type

from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    Serializer,
    StringDeserializer,
    StringSerializer,
)

from django_kafka import kafka
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.retry.settings import RetrySettings

if TYPE_CHECKING:
    from confluent_kafka import cimpl

logger = logging.getLogger(__name__)


class TopicProducer(ABC):
    key_serializer: Type[Serializer] = StringSerializer
    value_serializer: Type[Serializer] = StringSerializer

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
        headers: Optional[list] = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)

    def serialize(
        self,
        value,
        field: MessageField,
        headers: Optional[list] = None,
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
    key_deserializer: Type[Deserializer] = StringDeserializer
    value_deserializer: Type[Deserializer] = StringDeserializer

    retry_settings: Optional["RetrySettings"] = None

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
        headers: Optional[list] = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)

    def deserialize(
        self,
        value,
        field: MessageField,
        headers: Optional[list] = None,
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


class Topic(TopicConsumer, TopicProducer, ABC):
    """Combined Topic class for when topic consume and produce names are the same"""
