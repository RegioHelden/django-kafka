from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import MagicMock, Mock, call, patch

from django.test import SimpleTestCase, TestCase
from faker import Faker

from django_kafka import kafka
from django_kafka.consumer import Consumer, Topics
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.models import KeyOffsetTracker
from django_kafka.retry.tracker import KeyOffsetTrackerConsumer, KeyOffsetTrackerTopic
from django_kafka.tests.utils import message_mock
from django_kafka.topic import TopicConsumer
from django_kafka.utils.message import MessageTimestamp


class KeyOffsetTrackerConsumerTestCase(SimpleTestCase):
    @patch.multiple(
        "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
        config={},
        topics=None,
    )
    @patch("django_kafka.retry.tracker.kafka.consumers")
    @patch("django_kafka.retry.tracker.KeyOffsetTrackerConsumer.run_sanity_checks")
    @patch(
        "django_kafka.retry.tracker.KeyOffsetTrackerConsumer.tracked_topics",
        return_value=[Mock()],
    )
    def test_enable(
        self,
        mock_tracked_topics,
        mock_run_sanity_checks,
        mock_kafka_consumers,
    ):
        manager = MagicMock()
        manager.attach_mock(mock_tracked_topics, "tracked_topics")
        manager.attach_mock(mock_run_sanity_checks, "run_sanity_checks")
        manager.attach_mock(mock_kafka_consumers.return_value, "kafka_consumers")

        provided_group_id = "offset-tracker"
        self.assertIsNone(KeyOffsetTrackerConsumer.config.get("group.id"))
        KeyOffsetTrackerConsumer.activate(provided_group_id)
        mock_tracked_topics.assert_called_with()
        self.assertEqual(
            KeyOffsetTrackerConsumer.config.get("group.id"),
            provided_group_id,
        )
        self.assertEqual(
            KeyOffsetTrackerConsumer.build_config().get("group.id"),
            provided_group_id,
        )
        for topic in mock_tracked_topics.return_value:
            self.assertIn(topic, KeyOffsetTrackerConsumer.topics)

        mock_tracked_topics.assert_called_once_with()
        mock_run_sanity_checks.assert_called_once_with()
        mock_kafka_consumers.return_value.assert_called_once_with(
            KeyOffsetTrackerConsumer,
        )

        manager.assert_has_calls(
            [
                call.tracked_topics(),
                call.run_sanity_checks(),
                call.kafka_consumers(KeyOffsetTrackerConsumer),
            ],
            any_order=False,
        )

    def test_sanity_checks(self):
        no_group_id_msg = "'group.id' must be specified."
        no_topics_msg = (
            "Key offset tracker consumer is registered but there are no "
            "topics configured to use it."
        )
        with patch.multiple(
            "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
            config={"group.id": "project-key-offset-tracker"},
            topics=Topics(Mock()),
        ):
            # all required params are set, no errors raised
            KeyOffsetTrackerConsumer.run_sanity_checks()

        # nothing is set
        with self.assertRaises(DjangoKafkaError):
            KeyOffsetTrackerConsumer.run_sanity_checks()

        # group.id is not set
        with (
            self.assertRaisesMessage(DjangoKafkaError, no_group_id_msg),
            patch.multiple(
                "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
                topics=Topics(Mock()),
            ),
        ):
            KeyOffsetTrackerConsumer.run_sanity_checks()

            # topics are not set
            with (
                self.assertRaisesMessage(DjangoKafkaError, no_topics_msg),
                patch.multiple(
                    "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
                    config={"group.id": "project-key-offset-tracker"},
                ),
            ):
                KeyOffsetTrackerConsumer.run_sanity_checks()

        # topics are empty
        with (
            self.assertRaisesMessage(DjangoKafkaError, no_topics_msg),
            patch.multiple(
                "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
                config={"group.id": "project-key-offset-tracker"},
                topics=Topics(),
            ),
        ):
            KeyOffsetTrackerConsumer.run_sanity_checks()

    @patch.multiple(
        "django_kafka.retry.tracker.KeyOffsetTrackerConsumer",
        config={"group.id": "project-key-offset-tracker"},
    )
    @patch("django_kafka.consumer.consumer.ConfluentConsumer", new=Mock())
    @patch("django_kafka.retry.tracker.KeyOffsetTrackerConsumer.run_sanity_checks")
    @patch("django_kafka.retry.tracker.super")
    def start_runs_sanity_checks(self, mock_super, mock_run_sanity_checks):
        manager = MagicMock()
        manager.attach_mock(mock_super().start, "super")
        manager.attach_mock(mock_run_sanity_checks, "run_sanity_checks")

        KeyOffsetTrackerConsumer().start()

        mock_run_sanity_checks.assert_called_once_with()
        mock_super().start.assert_called_once_with()

        manager.assert_has_calls(
            [
                call.run_sanity_checks(),
                call.super(),
            ],
            any_order=False,
        )

    def test_tracked_topics(self):
        @kafka.retry(max_retries=3, delay=120, use_offset_tracker=True)
        class Topic1(TopicConsumer):
            name = "topic-1"

            def consume(self, msg):
                pass

        @kafka.retry(max_retries=3, delay=120, use_offset_tracker=True)
        class Topic2(TopicConsumer):
            name = "topic-2"

            def consume(self, msg):
                pass

        class SimpleTopic(TopicConsumer):
            name = "topic-3"

            def consume(self, msg):
                pass

        @kafka.consumers()
        class Consumer1(Consumer):
            config: ClassVar = {"group.id": "group_id1"}
            topics = Topics(Topic1(), SimpleTopic())

        @kafka.consumers()
        class Consumer2(Consumer):
            config: ClassVar = {"group.id": "group_id2"}
            topics = Topics(Topic2(), Topic1())

        topics_using_key_offset = (Topic1, Topic2)
        topics_not_using_key_offset = [SimpleTopic]

        tracked_topics = KeyOffsetTrackerConsumer.tracked_topics()

        self.assertListEqual(
            sorted([topic.name for topic in topics_using_key_offset]),
            sorted([topic.name for topic in tracked_topics]),
        )

        for topic in topics_not_using_key_offset:
            self.assertNotIn(topic.name, [t.name for t in tracked_topics])

        for topic in KeyOffsetTrackerConsumer.tracked_topics():
            self.assertIsInstance(topic, KeyOffsetTrackerTopic)


class TestKeyOffsetTrackerTopic(SimpleTestCase):
    def test_init_sets_topic_name(self):
        name = "topic-name"
        topic = KeyOffsetTrackerTopic(name)
        self.assertEqual(topic.name, name)

    @patch("django_kafka.models.KeyOffsetTracker.objects.log_msg_offset")
    def test_consume_logs_to_db(self, mock_log_msg_offset):
        msg = Mock()
        KeyOffsetTrackerTopic("topic-name").consume(msg)
        mock_log_msg_offset.assert_called_once_with(msg)


class KeyOffsetTrackerQuerySetTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()
        super().setUpClass()

    def test_log_msg_offset(self):
        now = datetime.now(UTC)
        timestamp = now.timestamp() * 1000
        msg1 = message_mock(
            offset=1,
            timestamp=[MessageTimestamp.CREATE_TIME, timestamp],
        )

        qs = KeyOffsetTracker.objects.all()

        instance, created = KeyOffsetTracker.objects.log_msg_offset(msg1)
        self.assertTrue(created)
        self.assertEqual(instance.offset, 1)
        self.assertEqual(instance.create_time, now)
        self.assertIsNone(instance.log_append_time)
        self.assertEqual(qs.count(), 1)

        msg1.offset.return_value = 2
        instance, created = KeyOffsetTracker.objects.log_msg_offset(msg1)
        self.assertFalse(created)
        self.assertEqual(instance.offset, 2)
        self.assertEqual(instance.create_time, now)
        self.assertIsNone(instance.log_append_time)
        self.assertEqual(qs.count(), 1)

        msg2 = message_mock(offset=100)
        instance, created = KeyOffsetTracker.objects.log_msg_offset(msg2)
        self.assertTrue(created)
        self.assertEqual(instance.offset, 100)
        self.assertIsNone(instance.create_time)
        self.assertIsNone(instance.log_append_time)
        self.assertEqual(qs.count(), 2)

    def test_has_future_offset(self):
        msg = message_mock()

        KeyOffsetTracker.objects.create(
            topic=msg.topic(),
            key=msg.key(),
            offset=msg.offset(),
        )
        self.assertFalse(KeyOffsetTracker.objects.has_future_offset(msg))

        KeyOffsetTracker.objects.update(offset=msg.offset() + 100)
        self.assertTrue(KeyOffsetTracker.objects.has_future_offset(msg))


class KeyOffsetTrackerModelTestCase(TestCase):
    def test_timestamp_property(self):
        now = datetime.now(UTC)
        timestamp = now.timestamp() * 1000
        instance = KeyOffsetTracker()

        instance.timestamp = [MessageTimestamp.CREATE_TIME, timestamp]
        self.assertEqual(instance.create_time, now)
        self.assertIsNone(instance.log_append_time)

        instance.timestamp = [MessageTimestamp.LOG_APPEND_TIME, timestamp]
        self.assertEqual(instance.log_append_time, now)
        self.assertIsNone(instance.create_time)

        instance.timestamp = [MessageTimestamp.NOT_AVAILABLE, timestamp]
        self.assertIsNone(instance.create_time)
        self.assertIsNone(instance.log_append_time)
