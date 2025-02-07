from unittest import mock
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from django_kafka.connect.connector import Connector
from django_kafka.consumer import Consumer, Topics
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.registry import ConnectorsRegistry, ConsumersRegistry, Registry


class RegistryTestCase(SimpleTestCase):
    def _gen_cls(self, name):
        return type(name, (object,), {})

    def test_registering_same_key_not_allowed(self):
        registry = Registry()

        cls = self._gen_cls("SomeClass")
        key = registry.get_key(cls)
        error_msg = f"`{key}` is already registered."

        registry()(cls)

        with self.assertRaisesMessage(DjangoKafkaError, error_msg):
            registry()(cls)

    def test_get_key(self):
        cls_a = self._gen_cls("ClassA")
        cls_b = self._gen_cls("ClassB")
        registry = Registry()

        key_a = registry.get_key(cls_a)
        self.assertEqual(
            key_a,
            f"{cls_a.__module__}.{cls_a.__name__}",
        )

        key_b = registry.get_key(cls_b)
        self.assertEqual(
            key_b,
            f"{cls_b.__module__}.{cls_b.__name__}",
        )

    def test_decorator_adds_to_registry(self):
        cls_a = self._gen_cls("ClassA")
        cls_b = self._gen_cls("classB")

        registry = Registry()

        self.assertIs(registry()(cls_a), cls_a)
        self.assertIs(registry()(cls_b), cls_b)

        key_a = registry.get_key(cls_a)
        self.assertIs(registry[key_a], cls_a)

        key_b = registry.get_key(cls_b)
        self.assertIs(registry[key_b], cls_b)

    def test_iter_returns_expected_keys(self):
        cls_a = self._gen_cls("ClassA")
        cls_b = self._gen_cls("ClassB")
        registry = Registry()

        registry()(cls_a)
        registry()(cls_b)

        key_a = registry.get_key(cls_a)
        key_b = registry.get_key(cls_b)

        self.assertCountEqual(list(registry), [key_a, key_b])


class ConsumersRegistryTestCase(SimpleTestCase):
    def _get_consumer_cls(self, name, group_id) -> type[Consumer]:
        return type[Consumer](
            name,
            (Consumer,),
            {"config": {"group.id": group_id}, "topics": Topics()},
        )

    @mock.patch("django_kafka.retry.consumer.RetryConsumer.build")
    def test_retry_consumer_registered(self, mock_retry_consumer_build):
        consumer_cls_a = self._get_consumer_cls("ConsumerA", "group_a")
        consumer_cls_b = self._get_consumer_cls("ConsumerB", "group_b")
        registry = ConsumersRegistry()
        retry_consumer_mock = mock.Mock()
        mock_retry_consumer_build.side_effect = [retry_consumer_mock, None]

        registry()(consumer_cls_a)
        registry()(consumer_cls_b)

        key_a = registry.get_key(consumer_cls_a)
        key_b = registry.get_key(consumer_cls_b)
        retry_key_a = f"{key_a}.retry"

        self.assertCountEqual(list(registry), [key_a, retry_key_a, key_b])
        self.assertIs(registry[retry_key_a], retry_consumer_mock)


@patch("django_kafka.connect.client.KafkaConnectSession", new=Mock())
class ConnectorsRegistryTestCase(SimpleTestCase):
    def _gen_cls(self, name) -> type[Connector]:
        return type[Connector](
            name,
            (Connector,),
            {"config": {}},
        )

    @patch.multiple("django_kafka.conf.settings", CONNECT_HOST="http://kafka-connect")
    def test_get_key(self):
        connector_cls = self._gen_cls("ConnectorCls")
        registry = ConnectorsRegistry()

        key = registry.get_key(connector_cls)
        self.assertEqual(key, connector_cls().name)

    def test_registering_same_key_not_allowed(self):
        registry = ConnectorsRegistry()
        connector_a = self._gen_cls("ConnectorA")
        # give the same class name to the connector to generate same key
        connector_b = self._gen_cls("ConnectorA")

        key = registry.get_key(connector_a)
        error_msg = f"`{key}` is already registered."

        registry()(connector_a)

        with self.assertRaisesMessage(DjangoKafkaError, error_msg):
            registry()(connector_b)
