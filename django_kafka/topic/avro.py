from abc import ABC

from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from django_kafka import kafka
from django_kafka.topic import Topic, TopicConsumer, TopicProducer


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
        topic.produce({"value": 1}, value_serializer_kwargs={
            "schema_str": json.dumps(schema)
        })
        ```

    [Cofluent AvroSerializer Config](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroserializer)
    [Avro schema definition](https://avro.apache.org/docs/1.11.1/specification/)
    """

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
    Defining schemas is not necessary as it gets retrieved
    automatically from the Schema Registry.
    """

    def get_key_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)

    def get_value_deserializer(self, **kwargs):
        return AvroDeserializer(kafka.schema_client, **kwargs)


class AvroTopic(AvroTopicConsumer, AvroTopicProducer, Topic, ABC):
    """Combined AvroTopic class for when topic consume and produce names are the same"""
