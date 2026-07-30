import datetime
from unittest import mock

from django.test import TestCase

from django_kafka.retry.settings import RetrySettings
from django_kafka.tests.utils import message_mock
from django_kafka.topic import Topic


class RetrySettingTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_msg = message_mock()
        super().setUpClass()

    def test_should_retry__include(self):
        settings = RetrySettings(max_retries=5, delay=60, include=[ValueError])

        self.assertEqual(
            settings.should_retry(self.mock_msg, attempt=0, exc=ValueError()),
            True,
        )
        self.assertEqual(
            settings.should_retry(self.mock_msg, attempt=0, exc=IndexError()),
            False,
        )

    def test_should_retry__exclude(self):
        settings = RetrySettings(max_retries=5, delay=60, exclude=[ValueError])

        self.assertEqual(
            settings.should_retry(self.mock_msg, attempt=0, exc=ValueError()),
            False,
        )
        self.assertEqual(
            settings.should_retry(self.mock_msg, attempt=0, exc=IndexError()),
            True,
        )

    def test_should_retry__no_include_exclude(self):
        settings = RetrySettings(max_retries=5, delay=60)

        self.assertEqual(
            settings.should_retry(self.mock_msg, attempt=0, exc=ValueError()),
            True,
        )

    def test_attempts_exceeded(self):
        settings = RetrySettings(max_retries=5, delay=60)

        self.assertEqual(settings.attempts_exceeded(6), True)
        self.assertEqual(settings.attempts_exceeded(5), False)
        self.assertEqual(settings.attempts_exceeded(4), False)

    @mock.patch("django.utils.timezone.now")
    def test_get_retry_time__fixed_delay(self, mock_now):
        settings = RetrySettings(max_retries=5, delay=60, backoff=False)
        now = datetime.datetime.fromtimestamp(1000, datetime.UTC)
        mock_now.return_value = now

        self.assertEqual(
            settings.get_retry_time(1),
            now + datetime.timedelta(seconds=60),
        )
        self.assertEqual(
            settings.get_retry_time(2),
            now + datetime.timedelta(seconds=60),
        )
        self.assertEqual(
            settings.get_retry_time(3),
            now + datetime.timedelta(seconds=60),
        )
        self.assertEqual(
            settings.get_retry_time(4),
            now + datetime.timedelta(seconds=60),
        )

    @mock.patch("django.utils.timezone.now")
    def test_get_retry_time__backoff_delay(self, mock_now):
        settings = RetrySettings(max_retries=5, delay=60, backoff=True)
        now = datetime.datetime.fromtimestamp(1000, datetime.UTC)
        mock_now.return_value = now

        self.assertEqual(
            settings.get_retry_time(1),
            now + datetime.timedelta(seconds=60),
        )
        self.assertEqual(
            settings.get_retry_time(2),
            now + datetime.timedelta(seconds=120),
        )
        self.assertEqual(
            settings.get_retry_time(3),
            now + datetime.timedelta(seconds=240),
        )
        self.assertEqual(
            settings.get_retry_time(4),
            now + datetime.timedelta(seconds=480),
        )

    def test_should_log(self):
        settings = RetrySettings(max_retries=5, delay=60, log_every=5)

        self.assertEqual(settings.should_log(1), False)
        self.assertEqual(settings.should_log(2), False)
        self.assertEqual(settings.should_log(5), True)
        self.assertEqual(settings.should_log(10), True)


class RetryDecoratorTestCase(TestCase):
    def test_retry_decorator(self):
        class TopicA(Topic):
            pass

        retry = RetrySettings(max_retries=5, delay=60)
        result = retry(TopicA)

        self.assertIs(result, TopicA)
        self.assertIsNotNone(TopicA.retry_settings)
        self.assertEqual(
            [TopicA.retry_settings.max_retries, TopicA.retry_settings.delay],
            [5, 60],
        )
