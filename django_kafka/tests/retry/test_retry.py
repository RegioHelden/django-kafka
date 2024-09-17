from unittest import mock

from django.test import TestCase

from django_kafka.retry.settings import RetrySettings
from django_kafka.topic import Topic


class RetrySettingTestCase(TestCase):
    def test_should_retry__include(self):
        settings = RetrySettings(max_retries=5, delay=60, include=[ValueError])

        self.assertEqual(settings.should_retry(ValueError()), True)
        self.assertEqual(settings.should_retry(IndexError()), False)

    def test_should_retry__exclude(self):
        settings = RetrySettings(max_retries=5, delay=60, exclude=[ValueError])

        self.assertEqual(settings.should_retry(ValueError()), False)
        self.assertEqual(settings.should_retry(IndexError()), True)

    def test_should_retry__no_include_exclude(self):
        settings = RetrySettings(max_retries=5, delay=60)

        self.assertEqual(settings.should_retry(ValueError()), True)

    def test_attempts_exceeded(self):
        settings = RetrySettings(max_retries=5, delay=60)

        self.assertEqual(settings.attempts_exceeded(6), True)
        self.assertEqual(settings.attempts_exceeded(5), False)
        self.assertEqual(settings.attempts_exceeded(4), False)

    @mock.patch("django.utils.timezone.now")
    def test_get_retry_time__fixed_delay(self, mock_now):
        settings = RetrySettings(max_retries=5, delay=60, backoff=False)

        timestamp = 1000
        mock_now.return_value.timestamp.return_value = timestamp
        self.assertEqual(settings.get_retry_timestamp(1), str(timestamp + 60))
        self.assertEqual(settings.get_retry_timestamp(2), str(timestamp + 60))
        self.assertEqual(settings.get_retry_timestamp(3), str(timestamp + 60))
        self.assertEqual(settings.get_retry_timestamp(4), str(timestamp + 60))

    @mock.patch("django.utils.timezone.now")
    def test_get_retry_time__backoff_delay(self, mock_now):
        settings = RetrySettings(max_retries=5, delay=60, backoff=True)

        timestamp = 1000
        mock_now.return_value.timestamp.return_value = timestamp
        self.assertEqual(settings.get_retry_timestamp(1), str(timestamp + 60))
        self.assertEqual(settings.get_retry_timestamp(2), str(timestamp + 120))
        self.assertEqual(settings.get_retry_timestamp(3), str(timestamp + 240))
        self.assertEqual(settings.get_retry_timestamp(4), str(timestamp + 480))


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
