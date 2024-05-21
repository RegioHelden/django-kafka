from django.test import TestCase

from django_kafka.consumer import Consumer
from django_kafka.registry import ConsumersRegistry


class ConsumersRegistryTestCase(TestCase):
    def test_decorator_adds_to_registry(self):
        class ConsumerA(Consumer):
            pass

        class ConsumerB(Consumer):
            pass

        registry = ConsumersRegistry()

        self.assertIs(registry()(ConsumerA), ConsumerA)
        self.assertIs(registry()(ConsumerB), ConsumerB)

        key_a = registry.get_key(ConsumerA)
        self.assertIs(registry[key_a], ConsumerA)

        key_b = registry.get_key(ConsumerB)
        self.assertIs(registry[key_b], ConsumerB)

    def test_iter_returns_keys(self):
        class ConsumerA(Consumer):
            pass

        class ConsumerB(Consumer):
            pass

        registry = ConsumersRegistry()

        registry()(ConsumerA)
        registry()(ConsumerB)

        self.assertListEqual(
            list(registry),
            [registry.get_key(ConsumerA), registry.get_key(ConsumerB)],
        )

    def test_get_key(self):
        class ConsumerA(Consumer):
            pass

        class ConsumerB(Consumer):
            pass

        registry = ConsumersRegistry()

        key_a = registry.get_key(ConsumerA)
        self.assertEqual(key_a, f"{ConsumerA.__module__}.{ConsumerA.__name__}")

        key_b = registry.get_key(ConsumerB)
        self.assertEqual(key_b, f"{ConsumerB.__module__}.{ConsumerB.__name__}")
