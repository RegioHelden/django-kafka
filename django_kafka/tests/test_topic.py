from unittest.mock import call, patch

from confluent_kafka.serialization import MessageField
from django.test import TestCase, override_settings

from django_kafka.conf import SETTINGS_KEY
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import AvroTopic, Topic


class SomeTopic(Topic):
    name = "some-topic"

    def consume(self, msg):
        pass


class TopicTestCase(TestCase):
    def setUp(self):
        self.topic = SomeTopic()

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.DjangoKafka.producer")
    def test_produce_only_value(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        headers = None  # default is None when not provided

        self.topic.produce(value)

        mock_topic_serialize.assert_called_once_with(value, MessageField.VALUE, headers)
        mock_kafka_producer.produce.assert_called_once_with(
            self.topic.name,
            mock_topic_serialize.return_value,
        )

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.DjangoKafka.producer")
    def test_produce_key_is_serialized(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        key = "some key"
        headers = {"header-1": "header-1-value"}

        self.topic.produce(value, key=key, headers=headers)

        self.assertEqual(
            mock_topic_serialize.call_args_list,
            [
                call(key, MessageField.KEY, headers),
                call(value, MessageField.VALUE, headers),
            ],
        )

        mock_kafka_producer.produce.assert_called_once_with(
            self.topic.name,
            mock_topic_serialize.return_value,
            key=mock_topic_serialize.return_value,
            headers=headers,
        )

    @patch("django_kafka.topic.Topic.key_deserializer")
    @patch("django_kafka.topic.Topic.context")
    def test_deserialize_key(self, mock_topic_context, mock_key_deserializer):
        value = b"some key"
        field = MessageField.KEY

        self.topic.deserialize(value, field)

        mock_topic_context.assert_called_once_with(field, None)
        mock_key_deserializer.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.value_deserializer")
    @patch("django_kafka.topic.Topic.context")
    def test_deserialize_value(self, mock_topic_context, mock_value_deserializer):
        value = b"some value"
        field = MessageField.VALUE

        self.topic.deserialize(value, field)

        mock_topic_context.assert_called_once_with(field, None)
        mock_value_deserializer.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.key_deserializer")
    @patch("django_kafka.topic.Topic.value_deserializer")
    @patch("django_kafka.topic.Topic.context")
    def test_deserialize_unknown_field(
        self,
        mock_topic_context,
        mock_value_deserializer,
        mock_key_deserializer,
    ):
        field = "something_unknown"

        with self.assertRaisesMessage(
            DjangoKafkaError,
            f"Unsupported deserialization field {field}.",
        ):
            self.topic.deserialize("some value", field)

        mock_topic_context.assert_not_called()
        mock_value_deserializer.assert_not_called()
        mock_key_deserializer.assert_not_called()

    @patch("django_kafka.topic.Topic.key_serializer")
    @patch("django_kafka.topic.Topic.context")
    def test_serialize_key(self, mock_topic_context, mock_key_serializer):
        value = "some key"
        field = MessageField.KEY

        self.topic.serialize(value, field)

        mock_topic_context.assert_called_once_with(field, None)
        mock_key_serializer.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.value_serializer")
    @patch("django_kafka.topic.Topic.context")
    def test_serialize_value(self, mock_topic_context, mock_value_serializer):
        value = "some value"
        field = MessageField.VALUE

        self.topic.serialize(value, field)

        mock_topic_context.assert_called_once_with(field, None)
        mock_value_serializer.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.key_serializer")
    @patch("django_kafka.topic.Topic.value_serializer")
    @patch("django_kafka.topic.Topic.context")
    def test_serialize_unknown_field(
        self,
        mock_topic_context,
        mock_value_serializer,
        mock_key_serializer,
    ):
        field = "something_unknown"

        with self.assertRaisesMessage(
            DjangoKafkaError,
            f"Unsupported serialization field {field}.",
        ):
            self.topic.serialize("some value", field)

        mock_topic_context.assert_not_called()
        mock_value_serializer.assert_not_called()
        mock_key_serializer.assert_not_called()

    def test_context(self):
        fields = [MessageField.VALUE, MessageField.KEY]
        headers = {"header-1": "header-1-value"}

        for field in fields:
            with patch(
                "django_kafka.topic.SerializationContext",
            ) as mock_serialization_context:
                self.topic.context(field, headers)
                mock_serialization_context.assert_called_once_with(
                    self.topic.name,
                    field,
                    headers=headers,
                )


class ATopic(AvroTopic):
    name = "some_topic_with_avro_serialization"

    def consume(self, msg):
        pass


@override_settings(
    **{SETTINGS_KEY: {"SCHEMA_REGISTRY": {"url": "http://schema-registy"}}},
)
@patch("django_kafka.topic.kafka.schema_client")
class AvroTopicTestCase(TestCase):
    def setUp(self):
        self.topic = ATopic()

    def test_key_schema(self, mock_kafka_schema_client):
        schema = self.topic.key_schema

        # return value of the schema.get_latest_version method call
        self.assertEqual(
            schema,
            mock_kafka_schema_client.get_latest_version.return_value,
        )
        # get_latest_version called with right arguments
        mock_kafka_schema_client.get_latest_version.assert_called_once_with(
            f"{self.topic.name}-key",
        )

    def test_value_schema(self, mock_kafka_schema_client):
        schema = self.topic.value_schema
        # return value of the schema.get_latest_version method call
        self.assertEqual(
            schema,
            mock_kafka_schema_client.get_latest_version.return_value,
        )
        # called with right arguments
        mock_kafka_schema_client.get_latest_version.assert_called_once_with(
            f"{self.topic.name}-value",
        )

    @patch("django_kafka.topic.AvroSerializer")
    def test_key_serializer(self, mock_avro_serializer, mock_kafka_schema_client):
        key_serializer = self.topic.key_serializer

        # returns AvroSerializer instance
        self.assertEqual(key_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            schema_str=self.topic.key_schema.schema.schema_str,
        )

    @patch("django_kafka.topic.AvroDeserializer")
    def test_key_deserializer(self, mock_avro_deserializer, mock_kafka_schema_client):
        key_deserializer = self.topic.key_deserializer

        # returns mock_AvroDeserializer instance
        self.assertEqual(key_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            schema_str=self.topic.key_schema.schema.schema_str,
        )

    @patch("django_kafka.topic.AvroSerializer")
    def test_value_serializer(self, mock_avro_serializer, mock_kafka_schema_client):
        value_serializer = self.topic.value_serializer

        # returns AvroSerializer instance
        self.assertEqual(value_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            schema_str=self.topic.key_schema.schema.schema_str,
        )

    @patch("django_kafka.topic.AvroDeserializer")
    def test_value_deserializer(self, mock_avro_deserializer, mock_kafka_schema_client):
        value_deserializer = self.topic.value_deserializer

        # returns mock_AvroDeserializer instance
        self.assertEqual(value_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            schema_str=self.topic.key_schema.schema.schema_str,
        )
