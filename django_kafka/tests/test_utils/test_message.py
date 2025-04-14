from django.test import SimpleTestCase

from django_kafka.utils.message import Header


class HeaderTestCase(SimpleTestCase):
    def test_get(self):
        headers = [("header", "abc"), ("header", "def")]

        self.assertEqual(Header.get(headers, "header"), "abc")
        self.assertEqual(Header.get(None, "header"), None)

    def test_list(self):
        headers = [("header", "abc"), ("header", "def")]

        self.assertEqual(Header.list(headers, "header"), ["abc", "def"])
        self.assertEqual(Header.list(None, "header"), [])
