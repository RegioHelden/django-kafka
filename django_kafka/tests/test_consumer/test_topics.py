from unittest.mock import Mock

from django.test import SimpleTestCase

from django_kafka.consumer import Topics
from django_kafka.retry.settings import RetrySettings


class TopicsTestCase(SimpleTestCase):
    def test_get_retryable(self):
        blocking_tc = Mock(
            retry_settings=RetrySettings(delay=1, max_retries=1, blocking=True),
        )
        non_blocking_tc = Mock(
            retry_settings=RetrySettings(delay=1, max_retries=1, blocking=False),
        )

        topics = Topics(blocking_tc, non_blocking_tc)

        self.assertEqual(topics.get_retryable(blocking=True), [blocking_tc])
        self.assertEqual(topics.get_retryable(blocking=False), [non_blocking_tc])
