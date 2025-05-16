import datetime
import traceback
from typing import ClassVar
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_kafka.conf import SETTINGS_KEY
from django_kafka.consumer import Consumer, Topics
from django_kafka.retry.consumer import RetryConsumer, RetryTopics
from django_kafka.retry.header import RetryHeader
from django_kafka.retry.settings import RetrySettings
from django_kafka.tests.utils import message_mock
from django_kafka.topic import TopicConsumer


class RetryConsumerTestCase(SimpleTestCase):
    def _get_topic_consumer(self):
        class SomeTopicConsumer(TopicConsumer):
            name = "normal_topic"

        return SomeTopicConsumer()

    def _get_retryable_topic_consumer(self):
        class RetryableTopicConsumer(TopicConsumer):
            name = "retry_topic"

        retry = RetrySettings(max_retries=5, delay=60, blocking=False)
        retry(RetryableTopicConsumer)

        return RetryableTopicConsumer()

    def _get_retryable_consumer_cls(self, group_id="group_id") -> type[Consumer]:
        class SomeConsumer(Consumer):
            topics = Topics(
                self._get_topic_consumer(),
                self._get_retryable_topic_consumer(),
            )
            config: ClassVar = {"group.id": group_id}

        return SomeConsumer

    def _get_retry_consumer(self, consumer_group_id="group_id") -> RetryConsumer:
        return RetryConsumer.build(
            self._get_retryable_consumer_cls(group_id=consumer_group_id),
        )()

    @override_settings(
        **{
            SETTINGS_KEY: {
                "RETRY_CONSUMER_CONFIG": {
                    "bootstrap.servers": "bootstrap.defined-by-retry-consumer-config",
                    "group.id": "group.id-defined-by-retry-consumer-config",
                    "topic.metadata.refresh.interval.ms": 10000,
                },
            },
        },
    )
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    @patch("django_kafka.retry.consumer.Consumer.build_config")
    def test_config_merge_override(
        self,
        mock_consumer_build_config,
        mock_consumer_client,
    ):
        """
        1. Consumer.build_config() is added to the consumers config
        2. RETRY_CONSUMER_CONFIG is merged next and overrides keys if any
        3. RetryConsumer.config is merged next and overrides keys if any
        """

        mock_consumer_build_config.return_value = {
            "bootstrap.servers": "bootstrap.defined-by-consumer-cls",
            "group.id": "group.id.set-by-consumer-cls",
            "enable.auto.offset.store": False,
        }

        class SomeRetryConsumer(RetryConsumer):
            config: ClassVar = {
                "group.id": "group.id.overridden-by-retry-consumer-class",
            }

        retry_consumer = SomeRetryConsumer()

        self.assertDictEqual(
            SomeRetryConsumer.build_config(),
            {
                "bootstrap.servers": "bootstrap.defined-by-retry-consumer-config",
                "group.id": "group.id.overridden-by-retry-consumer-class",
                "topic.metadata.refresh.interval.ms": 10000,
                "enable.auto.offset.store": False,
            },
        )
        self.assertDictEqual(
            SomeRetryConsumer.build_config(),
            retry_consumer.config,
        )

    def test_build(self):
        consumer_cls = self._get_retryable_consumer_cls()

        retry_consumer_cls = RetryConsumer.build(consumer_cls)

        self.assertTrue(issubclass(retry_consumer_cls, RetryConsumer))
        self.assertTrue(issubclass(retry_consumer_cls, consumer_cls))
        self.assertEqual(
            retry_consumer_cls.config["group.id"],
            f"{consumer_cls.build_config()['group.id']}.retry",
        )
        self.assertIsInstance(retry_consumer_cls.topics, RetryTopics)
        self.assertCountEqual(
            list(consumer_cls.topics.get_retryable(blocking=False)),
            [t.topic_consumer for t in retry_consumer_cls.topics],
        )

    def test_build__no_retry_topics(self):
        class TestConsumer(Consumer):
            topics = Topics()

        self.assertIsNone(RetryConsumer.build(TestConsumer))

    def test_retry_msg(self):
        mock_retry_topic_consumer = Mock()
        mock_get_producer_for = mock_retry_topic_consumer.get_producer_for
        mock_retry = mock_get_producer_for.return_value.retry
        msg_mock = message_mock()

        retry_consumer = self._get_retry_consumer()
        retry_consumer.get_topic = Mock(return_value=mock_retry_topic_consumer)
        exc = ValueError()

        retried, blocking = retry_consumer.retry_msg(msg_mock, exc)

        mock_get_producer_for.assert_called_once_with(msg_mock)
        mock_retry.assert_called_once_with(exc)
        self.assertEqual(retried, mock_retry.return_value)
        self.assertEqual(blocking, False)

    @patch("django_kafka.retry.consumer.DeadLetterTopicProducer")
    def test_dead_letter_msg(self, mock_dlt_topic_producer_cls):
        mock_retry_topic_consumer = Mock()
        mock_produce_for = mock_dlt_topic_producer_cls.return_value.produce_for
        msg_mock = message_mock()

        retry_consumer = self._get_retry_consumer()
        retry_consumer.get_topic = Mock(return_value=mock_retry_topic_consumer)
        exc = ValueError()

        retry_consumer.dead_letter_msg(msg_mock, exc)

        mock_dlt_topic_producer_cls.assert_called_once_with(
            group_id=mock_retry_topic_consumer.group_id,
            msg=msg_mock,
        )
        mock_produce_for.assert_called_once_with(
            header_summary=str(exc),
            header_detail=traceback.format_exc(),
        )

    @patch("django_kafka.consumer.Consumer.process_message")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message__before_retry_time(
        self,
        mock_confluent_consumer,
        mock_consumer_process_message,
    ):
        retry_consumer = self._get_retry_consumer()
        retry_consumer.pause_partition = Mock()
        retry_time = timezone.now() + datetime.timedelta(minutes=1)
        mock_msg = message_mock(
            headers=[
                (RetryHeader.TIMESTAMP, str(retry_time.timestamp())),
            ],
        )

        retry_consumer.process_message(mock_msg)
        retry_consumer.pause_partition.assert_called_once_with(mock_msg, retry_time)
        mock_consumer_process_message.process_message.assert_not_called()

    @patch("django_kafka.consumer.Consumer.process_message")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message__after_retry_time(
        self,
        mock_confluent_consumer,
        mock_consumer_process_message,
    ):
        retry_consumer = self._get_retry_consumer()
        retry_consumer.pause_partition = Mock()
        retry_time = timezone.now() - datetime.timedelta(minutes=1)
        mock_msg = message_mock(
            headers=[
                (RetryHeader.TIMESTAMP, str(retry_time.timestamp())),
            ],
        )

        retry_consumer.process_message(mock_msg)

        retry_consumer.pause_partition.assert_not_called()
        mock_consumer_process_message.assert_called_once_with(mock_msg)
