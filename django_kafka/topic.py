import logging
from abc import ABC, abstractmethod
from typing import Optional

from confluent_kafka import cimpl
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
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

logger = logging.getLogger(__name__)


class Topic(ABC):
    key_serializer: Serializer = StringSerializer()
    key_deserializer: Deserializer = StringDeserializer()

    value_serializer: Serializer = StringSerializer()
    value_deserializer: Deserializer = StringDeserializer()

    @property
    @abstractmethod
    def name(self) -> str:
        """Define Kafka topic name"""

    @abstractmethod
    def consume(self, msg: cimpl.Message):
        """Implement message processing"""

    def produce(self, value: any, **kwargs):
        headers = kwargs.get("headers")

        if "key" in kwargs:
            kwargs["key"] = self.serialize(kwargs["key"], MessageField.KEY, headers)

        kafka.producer.produce(
            self.name,
            self.serialize(value, MessageField.VALUE, headers),
            **kwargs,
        )

    def deserialize(
        self,
        value,
        field: MessageField,
        headers: Optional[dict | list] = None,
    ):
        if field == MessageField.VALUE:
            return self.value_deserializer(
                value,
                self.context(MessageField.VALUE, headers),
            )

        if field == MessageField.KEY:
            return self.key_deserializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported deserialization field {field}.")

    def serialize(
        self,
        value,
        field: MessageField,
        headers: Optional[dict | list] = None,
    ):
        if field == MessageField.VALUE:
            return self.value_serializer(
                value,
                self.context(MessageField.VALUE, headers),
            )

        if field == MessageField.KEY:
            return self.key_serializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported serialization field {field}.")

    def context(
        self,
        field: MessageField,
        headers: Optional[dict | list] = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)


class AvroTopic(Topic):
    serializer_conf: dict = None

    @property
    def key_schema(self) -> Optional[Schema | str]:
        return None

    @property
    def value_schema(self) -> Optional[Schema | str]:
        return None

    @property
    def key_serializer(self):
        return AvroSerializer(
            kafka.schema_client,
            schema_str=self.key_schema,
            conf=self.serializer_conf,
        )

    @property
    def value_serializer(self):
        return AvroSerializer(
            kafka.schema_client,
            schema_str=self.value_schema,
            conf=self.serializer_conf,
        )

    @property
    def key_deserializer(self):
        return AvroDeserializer(kafka.schema_client, schema_str=self.key_schema)

    @property
    def value_deserializer(self):
        return AvroDeserializer(kafka.schema_client, schema_str=self.value_schema)
