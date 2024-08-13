import logging
from abc import ABC, abstractmethod
from typing import Optional, Type

from confluent_kafka import cimpl
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
    key_serializer: Type[Serializer] = StringSerializer
    value_serializer: Type[Serializer] = StringSerializer

    key_deserializer: Type[Deserializer] = StringDeserializer
    value_deserializer: Type[Deserializer] = StringDeserializer

    @property
    @abstractmethod
    def name(self) -> str:
        """Define Kafka topic name"""

    def consume(self, msg: cimpl.Message):
        """Implement message processing"""
        raise NotImplementedError

    def produce(self, value: any, **kwargs):
        key_serializer_kwargs = kwargs.pop("key_serializer_kwargs", {}) or {}
        value_serializer_kwargs = kwargs.pop("value_serializer_kwargs", {}) or {}
        headers = kwargs.get("headers")

        if "key" in kwargs:
            kwargs["key"] = self.serialize(
                kwargs["key"], MessageField.KEY, headers, **key_serializer_kwargs)

        kafka.producer.produce(
            self.name,
            self.serialize(
                value, MessageField.VALUE, headers, **value_serializer_kwargs,
            ),
            **kwargs,
        )

    def serialize(
        self,
        value,
        field: MessageField,
        headers: Optional[dict | list] = None,
        **kwargs,
    ):
        if field == MessageField.VALUE:
            serializer = self.get_value_serializer(**kwargs)
            return serializer(value, self.context(MessageField.VALUE, headers))

        if field == MessageField.KEY:
            serializer = self.get_key_serializer(**kwargs)
            return serializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported serialization field {field}.")

    def deserialize(
        self,
        value,
        field: MessageField,
        headers: Optional[dict | list] = None,
        **kwargs,
    ):
        if field == MessageField.VALUE:
            deserializer = self.get_value_deserializer(**kwargs)
            return deserializer(value, self.context(MessageField.VALUE, headers))

        if field == MessageField.KEY:
            deserializer = self.get_key_deserializer(**kwargs)
            return deserializer(value, self.context(MessageField.KEY, headers))

        raise DjangoKafkaError(f"Unsupported deserialization field {field}.")

    def get_key_serializer(self, **kwargs):
        return self.key_serializer(**kwargs)

    def get_value_serializer(self, **kwargs):
        return self.value_serializer(**kwargs)

    def get_key_deserializer(self, **kwargs):
        return self.key_deserializer(**kwargs)

    def get_value_deserializer(self, **kwargs):
        return self.value_deserializer(**kwargs)

    def context(
        self,
        field: MessageField,
        headers: Optional[dict | list] = None,
    ) -> SerializationContext:
        return SerializationContext(self.name, field, headers=headers)


class AvroTopic(Topic, ABC):
    """
    Consume.
        Defining schemas is not necessary as it gets retrieved automatically from the Schema Registry.

    Produce.
        Defining `value_schema` is required (`key_schema` is required when using keys).
        It gets submitted to the Schema Registry

    Multiple schemas and one Topic:
        `AvroTopic.produce` takes `serializer_kwargs` kw argument.
        `AvroSerializer` then gets initialized with the provided kwargs.
        When producing you can tell which schema to use for your message:
        ```python
        schema = {
            "type": "record",
            "name": "ValueTest",
            "fields": [
                {"name": "value", "type": "string"},
            ]
        }
        topic.produce({"value": 1}, value_serializer_kwargs={"schema_str": json.dumps(schema)})
        ```

    [Cofluent AvroSerializer Config](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroserializer)
    [Avro schema definition](https://avro.apache.org/docs/1.11.1/specification/)
    """  # noqa: E501
    key_schema: str
    value_schema: str
    schema_config: dict

    def get_key_serializer(self, **kwargs):
        kwargs.setdefault("schema_str", getattr(self, "key_schema", None))
        kwargs.setdefault("conf", getattr(self, "schema_config", None))

        return AvroSerializer(kafka.schema_client, **kwargs)

    def get_value_serializer(self, **kwargs):
        kwargs.setdefault("schema_str", getattr(self, "value_schema", None))
        kwargs.setdefault("conf", getattr(self, "schema_config", None))

        return AvroSerializer(kafka.schema_client, **kwargs)

    def get_key_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)

    def get_value_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)
