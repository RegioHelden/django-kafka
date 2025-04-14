from unittest.mock import call, patch

from confluent_kafka.serialization import MessageField
from django.test import SimpleTestCase

from django_kafka import producer
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic import TopicConsumer, TopicProducer


class SomeTopicProducer(TopicProducer):
    name = "some-topic-producer"


class TopicProducerTestCase(SimpleTestCase):
    def setUp(self):
        self.topic_producer = SomeTopicProducer()

    def test_init__disallows_regex(self):
        class RegexTopicProducer(TopicProducer):
            name = "^topic"

        with self.assertRaises(DjangoKafkaError):
            RegexTopicProducer()

    @patch("django_kafka.topic.TopicProducer.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_serializer_kwargs(self, mock_kafka_producer, mock_topic_serialize):
        key = "key"
        value = "message value"
        headers = None  # default is None when not provided
        key_serializer_kwargs = {"a": "b"}
        value_serializer_kwargs = {"c": "d"}

        self.topic_producer.produce(
            value,
            key=key,
            key_serializer_kwargs=key_serializer_kwargs,
            value_serializer_kwargs=value_serializer_kwargs,
        )

        self.assertEqual(
            mock_topic_serialize.call_args_list,
            [
                call(
                    key,
                    MessageField.KEY,
                    headers,
                    **key_serializer_kwargs,
                ),
                call(
                    value,
                    MessageField.VALUE,
                    headers,
                    **value_serializer_kwargs,
                ),
            ],
        )

        mock_kafka_producer.produce.assert_called_once_with(
            self.topic_producer.name,
            mock_topic_serialize.return_value,
            key=mock_topic_serialize.return_value,
        )

    @patch("django_kafka.topic.TopicProducer.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_only_value(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        headers = None  # default is None when not provided

        self.topic_producer.produce(value)

        mock_topic_serialize.assert_called_once_with(
            value,
            MessageField.VALUE,
            headers,
        )
        mock_kafka_producer.produce.assert_called_once_with(
            self.topic_producer.name,
            mock_topic_serialize.return_value,
        )

    @patch("django_kafka.topic.TopicProducer.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_key_is_serialized(self, mock_kafka_producer, mock_topic_serialize):
        value = "message value"
        key = "some key"
        headers = {"header-1": "header-1-value"}

        self.topic_producer.produce(value, key=key, headers=headers)

        self.assertEqual(
            mock_topic_serialize.call_args_list,
            [
                call(key, MessageField.KEY, headers),
                call(value, MessageField.VALUE, headers),
            ],
        )

        mock_kafka_producer.produce.assert_called_once_with(
            self.topic_producer.name,
            mock_topic_serialize.return_value,
            key=mock_topic_serialize.return_value,
            headers=headers,
        )

    @patch("django_kafka.topic.TopicProducer.key_serializer")
    @patch("django_kafka.topic.TopicProducer.context")
    def test_serialize_key(self, mock_topic_context, mock_key_serializer):
        value = "some key"
        field = MessageField.KEY
        kwargs = {"key": "value"}

        self.topic_producer.serialize(value, field, **kwargs)

        mock_topic_context.assert_called_once_with(field, None)
        mock_key_serializer.assert_called_once_with(**kwargs)
        mock_key_serializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.TopicProducer.value_serializer")
    @patch("django_kafka.topic.TopicProducer.context")
    def test_serialize_value(self, mock_topic_context, mock_value_serializer):
        value = "some value"
        field = MessageField.VALUE
        kwargs = {"key": "value"}

        self.topic_producer.serialize(value, field, **kwargs)

        mock_topic_context.assert_called_once_with(field, None)
        mock_value_serializer.assert_called_once_with(**kwargs)
        mock_value_serializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.TopicProducer.key_serializer")
    @patch("django_kafka.topic.TopicProducer.value_serializer")
    @patch("django_kafka.topic.TopicProducer.context")
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
            self.topic_producer.serialize("some value", field)

        mock_topic_context.assert_not_called()
        mock_value_serializer.assert_not_called()
        mock_key_serializer.assert_not_called()

    @patch("django_kafka.topic.TopicProducer.serialize")
    @patch("django_kafka.kafka.producer")
    def test_produce_suppression(self, mock_kafka_producer, mock_topic_serialize):
        key = "key"
        value = "message value"

        with producer.suppress():
            self.topic_producer.produce(value, key=key)

        mock_topic_serialize.assert_not_called()
        mock_kafka_producer.produce.assert_not_called()

    def test_context(self):
        fields = [MessageField.VALUE, MessageField.KEY]
        headers = {"header-1": "header-1-value"}

        for field in fields:
            with patch(
                "django_kafka.topic.SerializationContext",
            ) as mock_serialization_context:
                self.topic_producer.context(field, headers)
                mock_serialization_context.assert_called_once_with(
                    self.topic_producer.name,
                    field,
                    headers=headers,
                )


class SomeTopicConsumer(TopicConsumer):
    name = "some-topic-consumer"

    def consume(self, msg):
        pass


class TopicConsumerTestCase(SimpleTestCase):
    def setUp(self):
        self.topic_consumer = SomeTopicConsumer()

    def test_is_regex(self):
        self.topic_consumer.name = "^some-topic"

        self.assertTrue(self.topic_consumer.is_regex())

    def test_matches(self):
        self.topic_consumer.name = "some-topic"

        self.assertTrue(self.topic_consumer.matches("some-topic"))
        self.assertFalse(self.topic_consumer.matches("some-topic.extra"))

    def test_matches__regex(self):
        self.topic_consumer.name = "^some-topic"

        self.assertTrue(self.topic_consumer.matches("some-topic"))
        self.assertTrue(self.topic_consumer.matches("some-topic.extra"))
        self.assertFalse(self.topic_consumer.matches("other-topic"))

    @patch("django_kafka.topic.TopicConsumer.key_deserializer")
    @patch("django_kafka.topic.TopicConsumer.context")
    def test_deserialize_key(self, mock_topic_context, mock_key_deserializer):
        value = b"some key"
        field = MessageField.KEY
        kwargs = {"key": "value"}

        self.topic_consumer.deserialize(value, field, **kwargs)

        mock_topic_context.assert_called_once_with(field, None)
        mock_key_deserializer.assert_called_once_with(**kwargs)
        mock_key_deserializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.TopicConsumer.value_deserializer")
    @patch("django_kafka.topic.TopicConsumer.context")
    def test_deserialize_value(self, mock_topic_context, mock_value_deserializer):
        value = b"some value"
        field = MessageField.VALUE
        kwargs = {"key": "value"}

        self.topic_consumer.deserialize(value, field, **kwargs)

        mock_topic_context.assert_called_once_with(field, None)
        mock_value_deserializer.assert_called_once_with(**kwargs)
        mock_value_deserializer.return_value.assert_called_once_with(
            value,
            mock_topic_context.return_value,
        )

    @patch("django_kafka.topic.TopicConsumer.key_deserializer")
    @patch("django_kafka.topic.TopicConsumer.value_deserializer")
    @patch("django_kafka.topic.TopicConsumer.context")
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
            self.topic_consumer.deserialize("some value", field)

        mock_topic_context.assert_not_called()
        mock_value_deserializer.assert_not_called()
        mock_key_deserializer.assert_not_called()

    def test_context(self):
        fields = [MessageField.VALUE, MessageField.KEY]
        headers = {"header-1": "header-1-value"}

        for field in fields:
            with patch(
                "django_kafka.topic.SerializationContext",
            ) as mock_serialization_context:
                self.topic_consumer.context(field, headers)
                mock_serialization_context.assert_called_once_with(
                    self.topic_consumer.name,
                    field,
                    headers=headers,
                )
