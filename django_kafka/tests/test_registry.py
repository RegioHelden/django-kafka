from typing import Type
from unittest import mock

from django.test import TestCase

from django_kafka.consumer import Consumer, Topics
from django_kafka.registry import ConsumersRegistry


class ConsumersRegistryTestCase(TestCase):
    def _get_consumer_cls(self, name, group_id) -> Type[Consumer]:
        return type[Consumer](
            name,
            (Consumer,),
            {"config": {"group.id": group_id}, "topics": Topics()},
        )

    def test_decorator_adds_to_registry(self):
        consumer_cls_a = self._get_consumer_cls("ConsumerA", "group_a")
        consumer_cls_b = self._get_consumer_cls("ConsumerB", "group_b")

        registry = ConsumersRegistry()

        self.assertIs(registry()(consumer_cls_a), consumer_cls_a)
        self.assertIs(registry()(consumer_cls_b), consumer_cls_b)

        key_a = registry.get_key(consumer_cls_a)
        self.assertIs(registry[key_a], consumer_cls_a)

        key_b = registry.get_key(consumer_cls_b)
        self.assertIs(registry[key_b], consumer_cls_b)

    def test_iter_returns_expected_keys(self):
        consumer_cls_a = self._get_consumer_cls("ConsumerA", "group_a")
        consumer_cls_b = self._get_consumer_cls("ConsumerB", "group_b")
        registry = ConsumersRegistry()

        registry()(consumer_cls_a)
        registry()(consumer_cls_b)

        key_a = registry.get_key(consumer_cls_a)
        key_b = registry.get_key(consumer_cls_b)

        self.assertCountEqual(list(registry), [key_a, key_b])

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

    def test_get_key(self):
        consumer_cls_a = self._get_consumer_cls("ConsumerA", "group_a")
        consumer_cls_b = self._get_consumer_cls("ConsumerB", "group_b")
        registry = ConsumersRegistry()

        key_a = registry.get_key(consumer_cls_a)
        self.assertEqual(
            key_a,
            f"{consumer_cls_a.__module__}.{consumer_cls_a.__name__}",
        )

        key_b = registry.get_key(consumer_cls_b)
        self.assertEqual(
            key_b,
            f"{consumer_cls_b.__module__}.{consumer_cls_b.__name__}",
        )
