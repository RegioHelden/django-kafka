from unittest import mock

from django.test import TestCase, override_settings
from django.utils import timezone

from django_kafka.conf import SETTINGS_KEY
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.retry.header import RetryHeader
from django_kafka.retry.settings import RetrySettings
from django_kafka.retry.topic import (
    RetryTopicConsumer,
    RetryTopicProducer,
)
from django_kafka.tests.utils import message_mock
from django_kafka.topic import TopicConsumer


@override_settings(**{SETTINGS_KEY: {"RETRY_TOPIC_SUFFIX": "test-retry"}})
class RetryTopicProducerTestCase(TestCase):
    def test__get_attempt(self):
        self.assertEqual(RetryTopicProducer.get_next_retry_attempt("topic"), 1)
        self.assertEqual(
            RetryTopicProducer.get_next_retry_attempt("group.id.topic.fake.5"),
            1,
        )
        self.assertEqual(
            RetryTopicProducer.get_next_retry_attempt("group.id.topic.test-retry"),
            1,
        )
        self.assertEqual(
            RetryTopicProducer.get_next_retry_attempt("group.id.topic.test-retry.0"),
            1,
        )
        self.assertEqual(
            RetryTopicProducer.get_next_retry_attempt("group.id.topic.test-retry.2"),
            3,
        )
        self.assertEqual(
            RetryTopicProducer.get_next_retry_attempt("group.id.topic.test-retry.10"),
            11,
        )

    def test_init(self):
        retry_settings = RetrySettings(max_retries=5, delay=60, blocking=False)
        mock_msg_topic_consumer = mock.Mock(**{"topic.return_value": "topic.name"})

        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=retry_settings,
            msg=mock_msg_topic_consumer,
        )

        self.assertEqual(rt_producer.group_id, "group.id")
        self.assertEqual(rt_producer.settings, retry_settings)
        self.assertEqual(rt_producer.msg, mock_msg_topic_consumer)
        self.assertEqual(rt_producer.retry_attempt, 1)

    def test_name(self):
        retry_settings = RetrySettings(max_retries=5, delay=60, blocking=False)
        mock_msg_topic_consumer = mock.Mock(**{"topic.return_value": "topic.name"})
        mock_msg_rt_producer = mock.Mock(
            **{
                "topic.return_value": "group.id.topic.name.test-retry.1",
            },
        )

        rt_producer_1 = RetryTopicProducer(
            group_id="group.id",
            retry_settings=retry_settings,
            msg=mock_msg_topic_consumer,
        )

        rt_producer_2 = RetryTopicProducer(
            group_id="group.id",
            retry_settings=retry_settings,
            msg=mock_msg_rt_producer,
        )

        self.assertEqual(rt_producer_1.name, "group.id.topic.name.test-retry.1")
        self.assertEqual(rt_producer_2.name, "group.id.topic.name.test-retry.2")

    @override_settings(**{SETTINGS_KEY: {"RETRY_TOPIC_SUFFIX": "test-retry"}})
    def test_name__uses_settings(self):
        retry_settings = RetrySettings(max_retries=5, delay=60, blocking=False)
        mock_msg_topic_consumer = mock.Mock(**{"topic.return_value": "topic.name"})

        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=retry_settings,
            msg=mock_msg_topic_consumer,
        )

        self.assertEqual(rt_producer.name, "group.id.topic.name.test-retry.1")

    @mock.patch("django_kafka.retry.settings.RetrySettings.get_retry_time")
    def test_retry__first_retry(self, mock_get_retry_time):
        retry_time = timezone.now()
        mock_get_retry_time.return_value = retry_time
        mock_msg = message_mock()
        retry_settings = RetrySettings(max_retries=5, delay=60, blocking=False)
        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=retry_settings,
            msg=mock_msg,
        )
        rt_producer.produce = mock.Mock()

        retried = rt_producer.retry(exc=ValueError("error message"))

        self.assertTrue(retried)
        rt_producer.produce.assert_called_with(
            key=mock_msg.key(),
            value=mock_msg.value(),
            headers=[
                (RetryHeader.MESSAGE, "error message"),
                (RetryHeader.TIMESTAMP, str(retry_time.timestamp())),
            ],
        )
        mock_get_retry_time.assert_called_once_with(1)

    @mock.patch("django_kafka.retry.settings.RetrySettings.get_retry_time")
    def test_retry__last_retry(self, mock_get_retry_time):
        retry_time = timezone.now()
        mock_get_retry_time.return_value = retry_time
        mock_msg = message_mock(topic="group.id.msg_topic.test-retry.4")
        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=RetrySettings(max_retries=5, delay=60, blocking=False),
            msg=mock_msg,
        )
        rt_producer.produce = mock.Mock()

        retried = rt_producer.retry(exc=ValueError("error message"))

        self.assertTrue(retried)
        rt_producer.produce.assert_called_with(
            key=mock_msg.key(),
            value=mock_msg.value(),
            headers=[
                (RetryHeader.MESSAGE, "error message"),
                (RetryHeader.TIMESTAMP, str(retry_time.timestamp())),
            ],
        )
        mock_get_retry_time.assert_called_once_with(5)

    def test_retry__no_more_retries(self):
        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=RetrySettings(max_retries=5, delay=60, blocking=False),
            msg=message_mock(topic="group.id.msg_topic.test-retry.5"),
        )
        rt_producer.produce = mock.Mock()

        retried = rt_producer.retry(exc=ValueError())

        self.assertFalse(retried)
        rt_producer.produce.assert_not_called()

    def test_retry__no_retry_excluded_error(self):
        rt_producer = RetryTopicProducer(
            group_id="group.id",
            retry_settings=RetrySettings(
                max_retries=5,
                delay=60,
                exclude=[ValueError],
                blocking=False,
            ),
            msg=message_mock(),
        )
        rt_producer.produce = mock.Mock()

        retried = rt_producer.retry(exc=ValueError())

        self.assertFalse(retried)
        rt_producer.produce.assert_not_called()


@override_settings(**{SETTINGS_KEY: {"RETRY_TOPIC_SUFFIX": "test-retry"}})
class RetryTopicConsumerTestCase(TestCase):
    def _get_retryable_topic_consumer(
        self,
        topic_name: str = "topic_name",
        **retry_kwargs,
    ):
        class SomeTopicConsumer(TopicConsumer):
            name = topic_name

        retry = RetrySettings(
            **{"max_retries": 5, "delay": 60, **retry_kwargs, "blocking": False},
        )
        topic_consumer_cls = retry(SomeTopicConsumer)
        return topic_consumer_cls()

    def test_init__raises_without_retry(self):
        class SomeTopicConsumer(TopicConsumer):
            name = "topic_name"

        with self.assertRaises(DjangoKafkaError):
            RetryTopicConsumer(group_id="group.id", topic_consumer=SomeTopicConsumer())

    def test_name(self):
        """assert name format and correct escaping of regex special characters"""
        topic_consumer = self._get_retryable_topic_consumer(topic_name="topic.name")
        rt_consumer = RetryTopicConsumer(
            group_id="group.id",
            topic_consumer=topic_consumer,
        )

        self.assertEqual(
            rt_consumer.name,
            r"^group\.id\.topic\.name\.test\-retry\.([0-9]+)$",
        )

    def test_name__regex(self):
        """tests regex topic names are correctly inserted in to retry topic regex"""
        topic_consumer = self._get_retryable_topic_consumer(
            topic_name="^topic_name|other_name$",
        )
        rt_consumer = RetryTopicConsumer(
            group_id="group.id",
            topic_consumer=topic_consumer,
        )

        self.assertEqual(
            rt_consumer.name,
            r"^group\.id\.(topic_name|other_name)\.test\-retry\.([0-9]+)$",
        )

    def test_consume(self):
        """tests RetryTopic uses the main topic consume method"""
        topic_consumer = self._get_retryable_topic_consumer()
        topic_consumer.consume = mock.Mock()
        mock_msg = message_mock()

        RetryTopicConsumer(group_id="group.id", topic_consumer=topic_consumer).consume(
            mock_msg,
        )

        topic_consumer.consume.assert_called_once_with(mock_msg)

    @mock.patch("django_kafka.retry.topic.RetryTopicProducer")
    def test_get_producer_for(self, mock_rt_producer):
        topic_consumer = self._get_retryable_topic_consumer()
        mock_msg = message_mock()

        rt_consumer = RetryTopicConsumer(
            group_id="group.id",
            topic_consumer=topic_consumer,
        )
        rt_producer = rt_consumer.get_producer_for(mock_msg)

        self.assertEqual(rt_producer, mock_rt_producer.return_value)
        mock_rt_producer.assert_called_once_with(
            retry_settings=topic_consumer.retry_settings,
            group_id=rt_consumer.group_id,
            msg=mock_msg,
        )
