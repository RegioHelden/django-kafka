from unittest.mock import patch

from django.test import TestCase, override_settings

from django_kafka import DjangoKafka
from django_kafka.conf import SETTINGS_KEY
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.producer import Producer
from django_kafka.registry import ConsumersRegistry


class DjangoKafkaTestCase(TestCase):
    def test_registry(self):
        kafka = DjangoKafka()

        # registry is there (registry itself is tested in a separate testcase)
        self.assertIsInstance(kafka.consumers, ConsumersRegistry)

    def test_producer(self):
        kafka = DjangoKafka()

        producer = kafka.producer
        # producer is there
        self.assertIsInstance(producer, Producer)
        # producer instance is the same (cached)
        self.assertIs(kafka.producer, producer)

    @patch("django_kafka.SchemaRegistryClient")
    def test_schema_client(self, mock_schema_registry_client):
        kafka = DjangoKafka()

        # exception when config is not provided
        with self.assertRaisesMessage(
            DjangoKafkaError,
            "`SCHEMA_REGISTRY` configuration is not defined.",
        ):
            # ruff: noqa: B018
            kafka.schema_client

        schema_registry_config = {"url": "http://schema-registry"}
        with override_settings(
            **{SETTINGS_KEY: {"SCHEMA_REGISTRY": schema_registry_config}},
        ):
            schema_client = kafka.schema_client
            # porper instance returned
            self.assertIs(schema_client, mock_schema_registry_client.return_value)
            # instantiated only once (cached)
            self.assertIs(kafka.schema_client, mock_schema_registry_client.return_value)
            # called with right args
            mock_schema_registry_client.assert_called_once_with(schema_registry_config)

    @patch("django_kafka.DjangoKafka.consumers")
    def test_start_consumer(self, mock_django_kafka_consumers):
        kafka = DjangoKafka()

        kafka.start_consumer("path.to.Consumer")

        mock_django_kafka_consumers[
            "path.to.Consumer"
        ].return_value.start.assert_called_once_with()
