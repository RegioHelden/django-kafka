import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Type

from confluent_kafka import cimpl
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    Serializer,
    StringSerializer,
)

from django_kafka import kafka
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.retry import RetrySettings

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
    key_deserializer: Type[Serializer] = StringSerializer
    value_deserializer: Type[Serializer] = StringSerializer

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

    def consume(self, msg: cimpl.Message):
        """Implement message processing"""
        raise NotImplementedError


class Topic(TopicConsumer, TopicProducer, ABC):
    """Combined Topic class for when topic consume and produce names are the same"""


class AvroTopicProducer(TopicProducer, ABC):
    """
    Defining `value_schema` is required (`key_schema` is required when using keys).
    It gets submitted to the Schema Registry.

    Multiple schemas and one Topic:
        `.produce(...)` takes `{key|value}_serializer_kwargs` kw argument.
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


class AvroTopicConsumer(TopicConsumer, ABC):
    """
    Defining schemas is not necessary as it gets retrieved automatically from the Schema Registry.
    """  # noqa: E501

    def get_key_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)

    def get_value_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)


class AvroTopic(AvroTopicConsumer, AvroTopicProducer, Topic, ABC):
    """Combined AvroTopic class for when topic consume and produce names are the same"""
