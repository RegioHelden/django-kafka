from django.test import TestCase
from django.utils import timezone

from django_kafka.retry.header import RetryHeader


class RetryHeaderTestCase(TestCase):
    def test_get_header(self):
        headers = [(RetryHeader.TIMESTAMP, "abc")]

        self.assertEqual(RetryHeader.get_header(headers, RetryHeader.TIMESTAMP), "abc")
        self.assertEqual(RetryHeader.get_header([], RetryHeader.TIMESTAMP), None)

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
