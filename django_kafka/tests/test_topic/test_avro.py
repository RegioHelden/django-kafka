from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from django_kafka.conf import SETTINGS_KEY
from django_kafka.topic.avro import AvroTopicConsumer, AvroTopicProducer


class ATopicProducer(AvroTopicProducer):
    name = "some_topic_with_avro_serialization"


@override_settings(
    **{SETTINGS_KEY: {"SCHEMA_REGISTRY": {"url": "http://schema-registy"}}},
)
@patch("django_kafka.topic.kafka.schema_client")
class AvroTopicTestCase(SimpleTestCase):
    def setUp(self):
        self.topic_producer = ATopicProducer()

    @patch("django_kafka.topic.avro.AvroSerializer")
    def test_get_key_serializer(
        self,
        mock_avro_serializer,
        mock_kafka_schema_client,
    ):
        kwargs = {
            "schema_str": "<some schema>",
            "conf": {},
        }
        key_serializer = self.topic_producer.get_key_serializer(**kwargs)

        # returns AvroSerializer instance
        self.assertEqual(key_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )

    @patch("django_kafka.topic.avro.AvroSerializer")
    def test_get_value_serializer(
        self,
        mock_avro_serializer,
        mock_kafka_schema_client,
    ):
        kwargs = {
            "schema_str": "<some schema>",
            "conf": {},
        }
        value_serializer = self.topic_producer.get_value_serializer(**kwargs)

        # returns AvroSerializer instance
        self.assertEqual(value_serializer, mock_avro_serializer.return_value)
        # instance was initialized with right arguments
        mock_avro_serializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )


class ATopicConsumer(AvroTopicConsumer):
    name = "some_topic_with_avro_deserialization"

    def consume(self, msg):
        pass


@override_settings(
    **{SETTINGS_KEY: {"SCHEMA_REGISTRY": {"url": "http://schema-registy"}}},
)
@patch("django_kafka.topic.kafka.schema_client")
class AvroTopicConsumerTestCase(SimpleTestCase):
    def setUp(self):
        self.topic_consumer = ATopicConsumer()

    @patch("django_kafka.topic.avro.AvroDeserializer")
    def test_get_key_deserializer(
        self,
        mock_avro_deserializer,
        mock_kafka_schema_client,
    ):
        kwargs = {}
        key_deserializer = self.topic_consumer.get_key_deserializer(**kwargs)

        # returns mock_AvroDeserializer instance
        self.assertEqual(key_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )

    @patch("django_kafka.topic.avro.AvroDeserializer")
    def test_get_value_deserializer(
        self,
        mock_avro_deserializer,
        mock_kafka_schema_client,
    ):
        kwargs = {}
        value_deserializer = self.topic_consumer.get_value_deserializer(**kwargs)

        # returns mock_AvroDeserializer instance
        self.assertEqual(value_deserializer, mock_avro_deserializer.return_value)
        # instance was initialized with right arguments
        mock_avro_deserializer.assert_called_once_with(
            mock_kafka_schema_client,
            **kwargs,
        )
