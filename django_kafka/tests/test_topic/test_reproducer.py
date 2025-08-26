from unittest import mock

from django.db.models import Model
from django.test import SimpleTestCase, TestCase

from django_kafka.tests.utils import message_mock
from django_kafka.topic.reproducer import ReproduceTopic, TopicReproducer


class ReproduceTopicTestCase(SimpleTestCase):
    def setUp(self):
        class _Topic(ReproduceTopic):
            reproducer = mock.Mock()
            name = "dummy"

        self.topic = _Topic()

    def test_consume_skips_when_kafka_skip_true_and_not_deletion(self):
        self.topic.deserialize = mock.Mock(
            side_effect=[
                {"id": 1},
                {"kafka_skip": True},
            ],
        )

        self.topic.consume(message_mock())

        self.topic.reproducer.reproduce.assert_not_called()

    def test_consume_calls_reproducer_on_deletion_when_value_is_none(self):
        key = {"id": 123}
        value = None
        self.topic.deserialize = mock.Mock(side_effect=[key, value])

        self.topic.consume(message_mock())

        self.topic.reproducer.reproduce.assert_called_once_with(key, value, True)

    def test_consume_calls_reproducer_on_deletion_flag_true(self):
        key = {"id": 5}
        value = {"__deleted": "true"}
        self.topic.deserialize = mock.Mock(side_effect=[key, value])

        self.topic.consume(message_mock())

        self.topic.reproducer.reproduce.assert_called_once_with(key, value, True)

    def test_consume_calls_reproducer_on_normal_event(self):
        key = {"id": 77}
        value = {"foo": "bar"}
        self.topic.deserialize = mock.Mock(side_effect=[key, value])

        self.topic.consume(message_mock())

        self.topic.reproducer.reproduce.assert_called_once_with(key, value, False)


class TopicReproducerGetTopicTestCase(SimpleTestCase):
    def setUp(self):
        class TestModel(Model):
            def __str__(self):
                return ""

        self.model = TestModel

    def test_get_reproduce_topic_with_name_and_namespace(self):
        class TestTopicReproducer(TopicReproducer):
            reproduce_name = "table"
            reproduce_namespace = "ns"

        topic = TestTopicReproducer.get_reproduce_topic()

        self.assertIsInstance(topic, ReproduceTopic)
        self.assertEqual(topic.name, "ns.table")
        self.assertIsInstance(topic.reproducer, TestTopicReproducer)

    def test_get_reproduce_topic_with_model(self):
        class TestTopicReproducer(TopicReproducer):
            reproduce_model = self.model
            reproduce_namespace = "ns"

        topic = TestTopicReproducer.get_reproduce_topic()

        self.assertIsInstance(topic, ReproduceTopic)
        self.assertEqual(topic.name, f"ns.{self.model._meta.db_table}")
        self.assertIsInstance(topic.reproducer, TestTopicReproducer)

    def test_get_reproduce_topic_raises_when_not_configured(self):
        class TestTopicReproducer(TopicReproducer):
            pass

        with self.assertRaises(ValueError):
            TestTopicReproducer.get_reproduce_topic()


class TopicReproducerBehaviourTestCase(TestCase):
    def setUp(self):
        class TestModel(Model):
            def __str__(self):
                return ""

        self.model = TestModel

    def test_reproduce_calls_deletion_when_is_deletion_true(self):
        class TestTopicReproducer(TopicReproducer):
            reproduce_model = self.model

        r = TestTopicReproducer()
        r._reproduce_deletion = mock.Mock()
        r._reproduce_upsert = mock.Mock()
        key = {"id": 10}
        value = None

        r.reproduce(key, value, is_deletion=True)

        r._reproduce_deletion.assert_called_once_with(10, key, value)
        r._reproduce_upsert.assert_not_called()

    def test_reproduce_calls_upsert_when_instance_exists(self):
        class TestTopicReproducer(TopicReproducer):
            reproduce_model = self.model

        self.model.objects.get = mock.Mock()
        r = TestTopicReproducer()
        r._reproduce_upsert = mock.Mock()
        r._reproduce_deletion = mock.Mock()
        key = {"id": 42}
        value = {"some": "data"}

        r.reproduce(key, value, is_deletion=False)

        self.model.objects.get.assert_called_once_with(id=42)
        r._reproduce_upsert.assert_called_once_with(
            self.model.objects.get.return_value,
            key,
            value,
        )
        r._reproduce_deletion.assert_not_called()

    def test_reproduce_suppresses_when_instance_missing(self):
        class TestTopicReproducer(TopicReproducer):
            reproduce_model = self.model

        self.model.objects.get = mock.Mock(side_effect=self.model.DoesNotExist)
        r = TestTopicReproducer()
        r._reproduce_upsert = mock.Mock()
        r._reproduce_deletion = mock.Mock()
        key = {"id": 1}
        value = {"x": 1}

        r.reproduce(key, value, is_deletion=False)

        self.model.objects.get.assert_called_once_with(id=1)
        r._reproduce_upsert.assert_not_called()
        r._reproduce_deletion.assert_not_called()
