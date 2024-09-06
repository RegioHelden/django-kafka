from unittest import mock

from django.test import TestCase

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.retry import RetrySettings
from django_kafka.retry.headers import RetryHeader
from django_kafka.retry.topic import RetryTopic
from django_kafka.topic import Topic


class RetryTopicTestCase(TestCase):
    def _get_retryable_topic(self, topic_name: str = "topic_name", **retry_kwargs):
        class TestTopic(Topic):
            name = topic_name

        retry = RetrySettings(**{"max_retries": 5, "delay": 60, **retry_kwargs})
        topic_cls = retry(TestTopic)
        return topic_cls()

    def test_init(self):
        main_topic = self._get_retryable_topic()

        RetryTopic(group_id="group.id", main_topic=main_topic)

    def test_init__raises_without_retry(self):
        class TestTopic(Topic):
            name = "topic_name"

        with self.assertRaises(DjangoKafkaError):
            RetryTopic(group_id="group.id", main_topic=TestTopic())

    def test__get_attempt(self):
        self.assertEqual(RetryTopic.get_attempt("topic"), 0)
        self.assertEqual(RetryTopic.get_attempt("group.id.topic.retry"), 0)
        self.assertEqual(RetryTopic.get_attempt("group.id.topic.retry.0"), 0)
        self.assertEqual(RetryTopic.get_attempt("group.id.topic.retry.2"), 2)
        self.assertEqual(RetryTopic.get_attempt("group.id.topic.retry.10"), 10)

    def test_name(self):
        """assert name format and correct escaping of regex special characters"""
        main_topic = self._get_retryable_topic(topic_name="topic.name")
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)

        self.assertEqual(retry_topic.name, r"^group\.id\.topic\.name\.retry\.[0-9]+$")

    def test_name__regex(self):
        """tests regex topic names are correctly inserted in to retry topic regex"""
        main_topic = self._get_retryable_topic(topic_name="^topic_name|other_name$")
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)

        self.assertEqual(
            retry_topic.name,
            r"^group\.id\.(topic_name|other_name)\.retry\.[0-9]+$",
        )

    def test_get_produce_name(self):
        main_topic = self._get_retryable_topic()
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)

        retry_topic_1 = retry_topic.get_produce_name("fake", 1)
        retry_topic_2 = retry_topic.get_produce_name("group.id.fake.retry.1", 2)
        retry_topic_3 = retry_topic.get_produce_name("group.id.fake.retry.2", 3)

        self.assertEqual(retry_topic_1, "group.id.fake.retry.1")
        self.assertEqual(retry_topic_2, "group.id.fake.retry.2")
        self.assertEqual(retry_topic_3, "group.id.fake.retry.3")

    def test_consume(self):
        """tests RetryTopic uses the main topic consume method"""
        main_topic = self._get_retryable_topic()
        main_topic.consume = mock.Mock()
        mock_msg = mock.Mock()

        RetryTopic(group_id="group.id", main_topic=main_topic).consume(mock_msg)

        main_topic.consume.assert_called_once_with(mock_msg)

    @mock.patch("django_kafka.retry.RetrySettings.get_retry_timestamp")
    def test_retry_for__first_retry(self, mock_get_retry_timestamp: mock.Mock):
        main_topic = self._get_retryable_topic(max_retries=5)
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)
        retry_topic.produce = mock.Mock()
        mock_msg = mock.Mock(**{"topic.return_value": "msg_topic"})

        retried = retry_topic.retry_for(
            msg=mock_msg,
            exc=ValueError("error message"),
        )

        self.assertTrue(retried)
        retry_topic.produce.assert_called_with(
            name="group.id.msg_topic.retry.1",
            key=mock_msg.key(),
            value=mock_msg.value(),
            headers=[
                (RetryHeader.MESSAGE, "error message"),
                (RetryHeader.TIMESTAMP, mock_get_retry_timestamp.return_value),
            ],
        )
        mock_get_retry_timestamp.assert_called_once_with(1)

    @mock.patch("django_kafka.retry.RetrySettings.get_retry_timestamp")
    def test_retry_for__last_retry(self, mock_get_retry_timestamp):
        main_topic = self._get_retryable_topic(max_retries=5)
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)
        retry_topic.produce = mock.Mock()
        mock_msg = mock.Mock(**{"topic.return_value": "group.id.msg_topic.retry.4"})

        retried = retry_topic.retry_for(
            msg=mock_msg,
            exc=ValueError("error message"),
        )

        self.assertTrue(retried)
        retry_topic.produce.assert_called_with(
            name="group.id.msg_topic.retry.5",
            key=mock_msg.key(),
            value=mock_msg.value(),
            headers=[
                (RetryHeader.MESSAGE, "error message"),
                (RetryHeader.TIMESTAMP, mock_get_retry_timestamp.return_value),
            ],
        )
        mock_get_retry_timestamp.assert_called_once_with(5)

    def test_retry_for__no_more_retries(self):
        main_topic = self._get_retryable_topic(max_retries=5)
        retry_topic = RetryTopic(group_id="group.id", main_topic=main_topic)
        retry_topic.produce = mock.Mock()
        mock_msg = mock.Mock(**{"topic.return_value": "group.id.msg_topic.retry.5"})

        retried = retry_topic.retry_for(
            msg=mock_msg,
            exc=ValueError(),
        )

        self.assertFalse(retried)
        retry_topic.produce.assert_not_called()

    def test_retry_for__no_retry_for_excluded_error(self):
        topic = self._get_retryable_topic(exclude=[ValueError])
        mock_msg = mock.Mock(**{"topic.return_value": "msg_topic"})
        retry_topic = RetryTopic(group_id="group.id", main_topic=topic)
        retry_topic.produce = mock.Mock()

        retried = retry_topic.retry_for(
            msg=mock_msg,
            exc=ValueError(),
        )

        self.assertFalse(retried)
        retry_topic.produce.assert_not_called()
