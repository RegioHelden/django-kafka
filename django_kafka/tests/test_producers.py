from unittest import mock
from unittest.mock import call

from django.test import TestCase

from django_kafka.producer import Producer, Suppression, suppress, unsuppress


@mock.patch("django_kafka.producer.ConfluentProducer")
class ProducerSuppressTestCase(TestCase):
    def test_suppression_active(self, mock_confluent_producer):
        # suppress should not be active if it hasn't been called
        self.assertFalse(Suppression.active("topicA"))

    def test_suppress_all(self, mock_confluent_producer):
        producer = Producer()

        with suppress():
            producer.produce("topicA")

        mock_confluent_producer.return_value.produce.assert_not_called()

    def test_suppress_topic_list(self, mock_confluent_producer):
        producer = Producer()

        with suppress(["topicA"]):
            producer.produce("topicA")
            producer.produce("topicB")

        mock_confluent_producer.return_value.produce.assert_called_once_with("topicB")

    def test_suppress_nested_usage(self, mock_confluent_producer):
        """tests that message suppression lists are combined with later contexts"""
        producer = Producer()

        with suppress(["topicA"]), suppress(["topicB"]):
            producer.produce("topicA")
            producer.produce("topicB")
            producer.produce("topicC")

        mock_confluent_producer.return_value.produce.assert_called_once_with("topicC")

    def test_suppress_nested_usage_all(self, mock_confluent_producer):
        """test that global message suppression is maintained by later contexts"""
        producer = Producer()

        with suppress(), suppress(["topicA"]):
            producer.produce("topicB")

        mock_confluent_producer.return_value.produce.assert_not_called()

    def test_unsuppress(self, mock_confluent_producer):
        producer = Producer()

        with suppress(["topicA"]), unsuppress():
            producer.produce("topicA")

        mock_confluent_producer.return_value.produce.assert_called_once_with("topicA")

    def test_unsuppress__decorator(self, mock_confluent_producer):
        producer = Producer()

        @suppress(["topicA"])
        @unsuppress()
        def _produce_empty_args():
            producer.produce("topicA")

        @suppress(["topicA"])
        @unsuppress
        def _produce_no_args():
            producer.produce("topicA")

        _produce_empty_args()
        _produce_no_args()

        mock_confluent_producer.return_value.produce.assert_has_calls(
            [call("topicA"), call("topicA")],
        )

    def test_suppress_resets(self, mock_confluent_producer):
        producer = Producer()

        with suppress(["topicA", "topicB"]):
            producer.produce("topicA")
        producer.produce("topicB")

        mock_confluent_producer.return_value.produce.assert_called_once_with("topicB")

    def test_suppress_usable_as_decorator(self, mock_confluent_producer):
        producer = Producer()

        @suppress(["topicA"])
        def _produce_args():
            producer.produce("topicA")

        @suppress()
        def _produce_empty_args():
            producer.produce("topicA")

        @suppress
        def _produce_no_args():
            producer.produce("topicA")

        _produce_args()
        _produce_empty_args()
        _produce_no_args()

        mock_confluent_producer.return_value.produce.assert_not_called()
