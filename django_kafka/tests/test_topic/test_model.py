from unittest import mock

from django.db import models

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.tests.models import AbstractModelTestCase
from django_kafka.topic.model import ModelTopicConsumer


class ModelTopicConsumerTestCase(AbstractModelTestCase):
    abstract_model = models.Model
    model: type[KafkaConnectSkipModel]

    def _get_model_topic_consumer(self):
        class SomeModelTopicConsumer(ModelTopicConsumer):
            name = "name"
            model = self.model

            def get_lookup_kwargs(self, model, key, value) -> dict:
                return {}

            def is_deletion(self, *args, **kwargs):
                return False

        return SomeModelTopicConsumer()

    def test_get_defaults(self):
        topic_consumer = self._get_model_topic_consumer()

        class SomeModel(models.Model):
            name = models.CharField()

            class Meta:
                abstract = True

        defaults = topic_consumer.get_defaults(
            model=SomeModel,
            value={"name": 1, "not_model_field": "value"},
        )

        self.assertEqual(defaults, {"name": 1})

    def test_get_defaults__adds_kafka_skip(self):
        topic_consumer = self._get_model_topic_consumer()

        class KafkaConnectSkip(KafkaConnectSkipModel):
            name = models.CharField()

        defaults = topic_consumer.get_defaults(
            model=KafkaConnectSkip,
            value={"name": 1},
        )

        self.assertEqual(defaults, {"name": 1, "kafka_skip": True})

    def test_transform(self):
        """test custom transform methods are used during transformation"""
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.transform_name = mock.Mock(
            return_value=("transformed_name", "transformed_value"),
        )

        transformed_value = topic_consumer.transform(self.model, {"name": "value"})

        self.assertEqual(transformed_value, {"transformed_name": "transformed_value"})
        topic_consumer.transform_name.assert_called_once_with(
            self.model,
            "name",
            "value",
        )

    def test_exclude_fields(self):
        """test excluded fields are not included in the transformed result"""
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.exclude_fields = ["name"]

        transformed_value = topic_consumer.transform(self.model, {"name": "value"})

        self.assertEqual(transformed_value, {})

    def test_sync(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.get_lookup_kwargs = mock.Mock(return_value={"id": "id"})
        topic_consumer.transform = mock.Mock()
        topic_consumer.get_defaults = mock.Mock(return_value={"name": "name"})
        model = mock.Mock()

        results = topic_consumer.sync(model, {"key": "key"}, {"value": "value"})

        topic_consumer.get_lookup_kwargs.assert_called_once_with(
            model,
            {"key": "key"},
            {"value": "value"},
        )
        topic_consumer.transform.assert_called_once_with(
            model,
            {"value": "value"},
        )
        topic_consumer.get_defaults.assert_called_once_with(
            model,
            topic_consumer.transform.return_value,
        )
        model.objects.update_or_create.assert_called_once_with(
            id="id",
            defaults={"name": "name"},
        )
        self.assertEqual(results, model.objects.update_or_create.return_value)

    def test_sync__deleted(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.get_lookup_kwargs = mock.Mock(return_value={"id": "id"})
        topic_consumer.get_defaults = mock.Mock()
        topic_consumer.is_deletion = mock.Mock(return_value=True)
        model = mock.Mock()

        results = topic_consumer.sync(model, {"key": "key"}, {"value": "value"})

        topic_consumer.get_lookup_kwargs.assert_called_once_with(
            model,
            {"key": "key"},
            {"value": "value"},
        )
        topic_consumer.get_defaults.assert_not_called()
        model.objects.get.assert_called_once_with(id="id")
        model.objects.get.return_value.delete.assert_called_once()
        model.objects.update_or_create.assert_not_called()
        self.assertIsNone(results)

    def test_get_model(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = mock.Mock()

        self.assertEqual(
            topic_consumer.get_model({}, {}),
            topic_consumer.model,
        )

    def test_get_model__raises_when_model_not_set(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = None

        with self.assertRaises(DjangoKafkaError):
            topic_consumer.get_model({}, {})

    def test_consume(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.get_model = mock.Mock()
        msg_key = {"key": "key"}
        msg_value = {"value": "value"}
        topic_consumer.deserialize = mock.Mock(side_effect=[msg_key, msg_value])
        topic_consumer.sync = mock.Mock()

        topic_consumer.consume(mock.Mock())

        topic_consumer.get_model.assert_called_once_with(msg_key, msg_value)
        topic_consumer.sync.assert_called_once_with(
            topic_consumer.get_model.return_value,
            msg_key,
            msg_value,
        )

    def test_model_has_field(self):
        class BookModel(models.Model):
            name = models.CharField(max_length=100)
            author = models.ForeignKey(self.model, models.CASCADE)

            def __str__(self):
                return self.name

        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = BookModel

        fields = ("id", "name", "author", "author_id")
        for field in fields:
            self.assertTrue(topic_consumer.model_has_field(BookModel, field))

        self.assertFalse(topic_consumer.model_has_field(BookModel, "not_defined_field"))
