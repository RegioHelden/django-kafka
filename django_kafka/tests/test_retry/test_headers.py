from django.test import SimpleTestCase
from django.utils import timezone

from django_kafka.retry.header import RetryHeader


class RetryHeaderTestCase(SimpleTestCase):
    def test_get_retry_time(self):
        now = timezone.now()
        headers = [(RetryHeader.TIMESTAMP, str(now.timestamp()))]

        self.assertEqual(RetryHeader.get_retry_time(headers), now)

    def test_get_retry_time__none_when_unavailable(self):
        headers1 = [(RetryHeader.TIMESTAMP, "abc")]
        headers2 = []
        headers3 = None

        self.assertEqual(RetryHeader.get_retry_time(headers1), None)
        self.assertEqual(RetryHeader.get_retry_time(headers2), None)
        self.assertEqual(RetryHeader.get_retry_time(headers3), None)
