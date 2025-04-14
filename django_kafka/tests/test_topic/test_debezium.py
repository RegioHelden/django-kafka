from unittest import mock

from django.db.models import Model
from django.test import SimpleTestCase

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic.debezium import DbzModelTopicConsumer


class DbzModelTopicConsumerTestCase(SimpleTestCase):
    def _get_model_topic_consumer(self) -> DbzModelTopicConsumer:
        class SomeModelTopicConsumer(DbzModelTopicConsumer):
            name = "name"
            model = Model

        return SomeModelTopicConsumer()

    def test_get_model__uses_reroute_model_map(self):
        mock_model = mock.Mock()
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.reroute_model_map = {"topic": mock_model}
        topic_consumer.reroute_key_field_name = "table_identifier_key"

        self.assertEqual(
            topic_consumer.get_model({"table_identifier_key": "topic"}, {}),
            mock_model,
        )

    def test_get_model__uses_direct_model(self):
        mock_model = mock.Mock()
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = mock_model
        topic_consumer.reroute_model_map = None

        self.assertEqual(topic_consumer.get_model({}, {}), mock_model)

    def test_get_model__raises_when_nothing_set(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = None
        topic_consumer.reroute_model_map = None

        with self.assertRaises(DjangoKafkaError):
            topic_consumer.get_model({}, {})

    def test_get_model__raises_when_reroute_key_present(self):
        topic_consumer = self._get_model_topic_consumer()
        topic_consumer.model = Model
        topic_consumer.reroute_model_map = None
        topic_consumer.reroute_key_field_name = "table_identifier_key"

        with self.assertRaises(DjangoKafkaError):
            topic_consumer.get_model({"table_identifier_key": "table"}, {})

    def test_is_deletion(self):
        topic_consumer = self._get_model_topic_consumer()

        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {}),
            False,
        )
        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {"__deleted": "true"}),
            True,
        )
        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {"__deleted": "True"}),
            True,
        )
        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {"__deleted": "false"}),
            False,
        )
        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {"__deleted": "anything"}),
            False,
        )
        self.assertEqual(
            topic_consumer.is_deletion(Model, {}, {"__deleted": False}),
            False,
        )
        self.assertEqual(
            topic_consumer.is_deletion(
                Model,
                {},
                {"name": "name", "__deleted": True},
            ),
            True,
        )
        self.assertEqual(topic_consumer.is_deletion(Model, {}, None), True)

    def test_get_lookup_kwargs(self):
        topic_consumer = self._get_model_topic_consumer()
        mock_model = mock.Mock(**{"_meta.pk.name": "identity_key"})

        self.assertEqual(
            topic_consumer.get_lookup_kwargs(mock_model, {"identity_key": 1}, {}),
            {"identity_key": 1},
        )

    def test_get_lookup_kwargs__raises_when_pk_not_present(self):
        topic_consumer = self._get_model_topic_consumer()
        mock_model = mock.Mock(**{"_meta.pk.name": "identity_key"})

        with self.assertRaises(KeyError):
            topic_consumer.get_lookup_kwargs(mock_model, {"something": 1}, {})
