import datetime

from django.test import SimpleTestCase
from django.utils import timezone

from django_kafka.consumer.managers import PauseManager, RetryManager
from django_kafka.tests.utils import message_mock


class PauseManagerTestCase(SimpleTestCase):
    def test_get_msg_partition(self):
        mock_msg = message_mock()
        manager = PauseManager()

        tp = manager.get_msg_partition(mock_msg)

        self.assertEqual(tp.topic, mock_msg.topic())
        self.assertEqual(tp.partition, mock_msg.partition())

    def test_set(self):
        mock_msg = message_mock()
        manager = PauseManager()

        tp = manager.set(mock_msg, timezone.now())

        self.assertEqual(manager.get_msg_partition(mock_msg), tp)

    def test_pop_ready(self):
        manager = PauseManager()
        mock_msg_1 = message_mock(partition=1)
        mock_msg_2 = message_mock(partition=2)

        manager.set(mock_msg_1, timezone.now() - datetime.timedelta(minutes=1))
        manager.set(mock_msg_2, timezone.now() + datetime.timedelta(minutes=1))

        self.assertEqual(
            list(manager.pop_ready()),
            [manager.get_msg_partition(mock_msg_1)],
        )
        self.assertEqual(list(manager.pop_ready()), [])  # empty the second time


class RetryManagerTestCase(SimpleTestCase):
    def test_get_msg_partition(self):
        mock_msg = message_mock()
        manager = RetryManager()

        tp = manager.get_msg_partition(mock_msg)

        self.assertEqual(tp.topic, mock_msg.topic())
        self.assertEqual(tp.partition, mock_msg.partition())
        self.assertEqual(tp.offset, mock_msg.offset())

    def test_next(self):
        mock_msg = message_mock()
        manager = RetryManager()

        self.assertEqual(manager.next(mock_msg), 1)
        self.assertEqual(manager.next(mock_msg), 2)
        self.assertEqual(manager.next(mock_msg), 3)

    def test_next__resets_for_new_offset(self):
        """tests retry attempt resets if requested for a new offset"""
        mock_msg = message_mock(offset=0)
        manager = RetryManager()

        self.assertEqual(manager.next(mock_msg), 1)

        mock_msg.offset.return_value = 1

        self.assertEqual(manager.next(mock_msg), 1)
        self.assertEqual(manager.next(mock_msg), 2)
