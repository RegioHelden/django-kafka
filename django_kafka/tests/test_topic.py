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

    def test_is_regex(self):
        self.topic.name = "^some-topic"

        self.assertTrue(self.topic.is_regex())

    def test_matches(self):
        self.topic.name = "some-topic"

        self.assertTrue(self.topic.matches("some-topic"))
        self.assertFalse(self.topic.matches("some-topic.extra"))

    def test_matches__regex(self):
        self.topic.name = "^some-topic"

        self.assertTrue(self.topic.matches("some-topic"))
        self.assertTrue(self.topic.matches("some-topic.extra"))
        self.assertFalse(self.topic.matches("other-topic"))

    def test_validate_produce_name(self):
        """validates and returns expected producing name for non-regex based topics"""
        self.assertEqual(self.topic.name, self.topic.validate_produce_name(None))
        self.assertEqual(
            self.topic.name, self.topic.validate_produce_name(self.topic.name)
        )
        with self.assertRaisesMessage(
            DjangoKafkaError,
            "topic producing name `invalid-topic` is not valid for this topic",
        ):
            self.topic.validate_produce_name("invalid-topic")

    def test_validate_produce_name__regex(self):
        """validates supplied producing name when topic is regex based"""
        self.topic.name = "^some-topic"

        self.assertEqual(
            "some-topic.extra",
            self.topic.validate_produce_name("some-topic.extra"),
        )
        with self.assertRaisesMessage(
            DjangoKafkaError,
            "topic producing name `invalid-topic` is not valid for this topic",
        ):
            self.topic.validate_produce_name("invalid-topic")
        with self.assertRaisesMessage(
            DjangoKafkaError,
            "topic producing name must be supplied for regex-based topics",
        ):
            self.topic.validate_produce_name(None)

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_serializer_kwargs(self, mock_kafka_producer, mock_topic_serialize):
        self.topic.name = "^some-topic"
        name = "some-topic.extra"
        key = "key"
        value = "message value"
        headers = None  # default is None when not provided
        key_serializer_kwargs = {"a": "b"}
        value_serializer_kwargs = {"c": "d"}

        self.topic.produce(
            value,
            name=name,
            key=key,
            key_serializer_kwargs=key_serializer_kwargs,
            value_serializer_kwargs=value_serializer_kwargs,
        )

        self.assertEqual(
            mock_topic_serialize.call_args_list,
            [
                call(
                    name,
                    key,
                    MessageField.KEY,
                    headers,
                    **key_serializer_kwargs,
                ),
                call(
                    name,
                    value,
                    MessageField.VALUE,
                    headers,
                    **value_serializer_kwargs,
                ),
            ],
        )

        mock_kafka_producer.produce.assert_called_once_with(
            name,
            mock_topic_serialize.return_value,
            key=mock_topic_serialize.return_value,
        )

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_requires_kwargs_supplied_name_for_regex_names(
        self,
        mock_kafka_producer,
        mock_topic_serialize,
    ):
        self.topic.name = "^some-topic"
        value = "message value"

        with self.assertRaisesMessage(
            DjangoKafkaError,
            "topic producing name must be supplied for regex-based topics",
        ):
            self.topic.produce(value)

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_only_value(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        headers = None  # default is None when not provided

        self.topic.produce(value)

        mock_topic_serialize.assert_called_once_with(
            self.topic.name,
            value,
            MessageField.VALUE,
            headers,
        )
        mock_kafka_producer.produce.assert_called_once_with(
            self.topic.name,
            mock_topic_serialize.return_value,
        )

    @patch("django_kafka.topic.Topic.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_key_is_serialized(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        key = "some key"
        headers = {"header-1": "header-1-value"}

        self.topic.produce(value, key=key, headers=headers)

        self.assertEqual(
            mock_topic_serialize.call_args_list,
            [
                call(self.topic.name, key, MessageField.KEY, headers),
                call(self.topic.name, value, MessageField.VALUE, headers),
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
        name = "name"
        value = b"some key"
        field = MessageField.KEY
        kwargs = {"key": "value"}

        self.topic.deserialize(name, value, field, **kwargs)

        mock_topic_context.assert_called_once_with(name, field, None)
        mock_key_deserializer.assert_called_once_with(**kwargs)
        mock_key_deserializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.value_deserializer")
    @patch("django_kafka.topic.Topic.context")
    def test_deserialize_value(self, mock_topic_context, mock_value_deserializer):
        name = "name"
        value = b"some value"
        field = MessageField.VALUE
        kwargs = {"key": "value"}

        self.topic.deserialize(name, value, field, **kwargs)

        mock_topic_context.assert_called_once_with(name, field, None)
        mock_value_deserializer.assert_called_once_with(**kwargs)
        mock_value_deserializer.return_value.assert_called_once_with(
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
            self.topic.deserialize("name", "some value", field)

        mock_topic_context.assert_not_called()
        mock_value_deserializer.assert_not_called()
        mock_key_deserializer.assert_not_called()

    @patch("django_kafka.topic.Topic.key_serializer")
    @patch("django_kafka.topic.Topic.context")
    def test_serialize_key(self, mock_topic_context, mock_key_serializer):
        name = "name"
        value = "some key"
        field = MessageField.KEY
        kwargs = {"key": "value"}

        self.topic.serialize(name, value, field, **kwargs)

        mock_topic_context.assert_called_once_with(name, field, None)
        mock_key_serializer.assert_called_once_with(**kwargs)
        mock_key_serializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.Topic.value_serializer")
    @patch("django_kafka.topic.Topic.context")
    def test_serialize_value(self, mock_topic_context, mock_value_serializer):
        name = "name"
        value = "some value"
        field = MessageField.VALUE
        kwargs = {"key": "value"}

        self.topic.serialize(name, value, field, **kwargs)

        mock_topic_context.assert_called_once_with(name, field, None)
        mock_value_serializer.assert_called_once_with(**kwargs)
        mock_value_serializer.return_value.assert_called_once_with(
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
            self.topic.serialize("name", "some value", field)

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
                self.topic.context(self.topic.name, field, headers)
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

    @patch("django_kafka.topic.AvroSerializer")
    def test_get_key_serializer(
        self,
        mock_avro_serializer,
        mock_kafka_schema_client,
    ):
        kwargs = {
            "schema_str": "<some schema>",
            "conf": {},
        }
        key_serializer = self.topic.get_key_serializer(**kwargs)

        # returns AvroSerializer instance
        self.assertEqual(key_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )

    @patch("django_kafka.topic.AvroDeserializer")
    def test_get_key_deserializer(
        self,
        mock_avro_deserializer,
        mock_kafka_schema_client,
    ):
        kwargs = {}
        key_deserializer = self.topic.get_key_deserializer(**kwargs)

        # returns mock_AvroDeserializer instance
        self.assertEqual(key_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )

    @patch("django_kafka.topic.AvroSerializer")
    def test_get_value_serializer(
        self,
        mock_avro_serializer,
        mock_kafka_schema_client,
    ):
        kwargs = {
            "schema_str": "<some schema>",
            "conf": {},
        }
        value_serializer = self.topic.get_value_serializer(**kwargs)

        # returns AvroSerializer instance
        self.assertEqual(value_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )

    @patch("django_kafka.topic.AvroDeserializer")
    def test_get_value_deserializer(
        self,
        mock_avro_deserializer,
        mock_kafka_schema_client,
    ):
        kwargs = {}
        value_deserializer = self.topic.get_value_deserializer(**kwargs)

        # returns mock_AvroDeserializer instance
        self.assertEqual(value_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )
