from unittest import mock

from django.test import TestCase, override_settings

from django_kafka.conf import SETTINGS_KEY
from django_kafka.dead_letter.header import DeadLetterHeader
from django_kafka.dead_letter.topic import DeadLetterTopicProducer


@override_settings(
    **{
        SETTINGS_KEY: {
            "RETRY_TOPIC_SUFFIX": "test-retry",
            "DEAD_LETTER_TOPIC_SUFFIX": "test-dlt",
        },
    },
)
class DeadLetterTopicProducerTestCase(TestCase):
    def test_name(self):
        mock_msg_topic_consumer = mock.Mock(**{"topic.return_value": "topic.name"})
        mock_msg_retry_topic = mock.Mock(
            **{"topic.return_value": "group.id.topic.name.test-retry.10"},
        )

        dlt_producer_1 = DeadLetterTopicProducer(
            group_id="group.id",
            msg=mock_msg_topic_consumer,
        )

        dlt_producer_2 = DeadLetterTopicProducer(
            group_id="group.id",
            msg=mock_msg_retry_topic,
        )

        self.assertEqual(dlt_producer_1.name, "group.id.topic.name.test-dlt")
        self.assertEqual(dlt_producer_2.name, "group.id.topic.name.test-dlt")

    def test_produce_for(self):
        msg_mock = mock.Mock(**{"topic.return_value": "msg_topic"})
        dlt_producer = DeadLetterTopicProducer(group_id="group.id", msg=msg_mock)
        dlt_producer.produce = mock.Mock()
        header_message = "header message"
        header_detail = "header detail"

        dlt_producer.produce_for(
            header_message=header_message,
            header_detail=header_detail,
        )

        dlt_producer.produce.assert_called_once_with(
            key=msg_mock.key(),
            value=msg_mock.value(),
            headers=[
                (DeadLetterHeader.MESSAGE, header_message),
                (DeadLetterHeader.DETAIL, header_detail),
            ],
        )
