import datetime
from unittest import mock

from django.test import TestCase

from django_kafka.models import KeyOffsetTracker
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

    def test_skip_by_offset(self):
        msg = message_mock()
        model_instance = KeyOffsetTracker.objects.create(
            topic=msg.topic(),
            key=msg.key(),
            offset=msg.offset(),
            timestamp=msg.timestamp(),
        )

        # not configured to use offset
        settings = RetrySettings(
            max_retries=5,
            delay=60,
            backoff=False,
            use_offset_tracker=False,
        )
        for error in settings.relational_errors:
            self.assertFalse(settings.skip_by_offset(msg, error()))

        # configured to use offset, but exception is not among accepted
        settings = RetrySettings(
            max_retries=5,
            delay=60,
            backoff=False,
            use_offset_tracker=True,
        )
        self.assertFalse(settings.skip_by_offset(msg, Exception()))

        # configured to use offset, exceptions are valid but no future offset
        settings = RetrySettings(
            max_retries=5,
            delay=60,
            backoff=False,
            use_offset_tracker=True,
        )
        for error in settings.relational_errors:
            self.assertFalse(settings.skip_by_offset(self.mock_msg, error()))

        # configured to use offset and everything is aligned
        settings = RetrySettings(
            max_retries=5,
            delay=60,
            backoff=False,
            use_offset_tracker=True,
        )

        model_instance.offset += 1
        model_instance.save(update_fields=["offset"])

        for error in settings.relational_errors:
            self.assertTrue(settings.skip_by_offset(msg, error()))

    @mock.patch(
        "django_kafka.retry.settings.RetrySettings.skip_by_offset",
        return_value=True,
    )
    def test_should_retry_calls_skip_by_offset(self, skip_by_offset):
        settings = RetrySettings(
            max_retries=5,
            delay=60,
            backoff=False,
            use_offset_tracker=True,
        )
        error = Exception()
        result = settings.should_retry(self.mock_msg, 1, error)

        skip_by_offset.assert_called_once_with(self.mock_msg, error)
        self.assertIs(result, False)


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
