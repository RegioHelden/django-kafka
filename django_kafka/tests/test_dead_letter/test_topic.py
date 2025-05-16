from unittest import mock

from django.test import SimpleTestCase, override_settings

from django_kafka.conf import SETTINGS_KEY
from django_kafka.dead_letter.header import DeadLetterHeader
from django_kafka.dead_letter.topic import DeadLetterTopicProducer
from django_kafka.tests.utils import message_mock


@override_settings(
    **{
        SETTINGS_KEY: {
            "RETRY_TOPIC_SUFFIX": "test-retry",
            "DEAD_LETTER_TOPIC_SUFFIX": "test-dlt",
        },
    },
)
class DeadLetterTopicProducerTestCase(SimpleTestCase):
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
        msg_mock = message_mock(topic="topic.name", partition=5, offset=1)
        dlt_producer = DeadLetterTopicProducer(group_id="group.id", msg=msg_mock)
        dlt_producer.produce = mock.Mock()
        header_summary = "header summary"
        header_detail = "header detail"

        dlt_producer.produce_for(
            header_summary=header_summary,
            header_detail=header_detail,
        )

        dlt_producer.produce.assert_called_once_with(
            key=msg_mock.key(),
            value=msg_mock.value(),
            headers=[
                (DeadLetterHeader.MESSAGE_TOPIC, "topic.name"),
                (DeadLetterHeader.MESSAGE_PARTITION, "5"),
                (DeadLetterHeader.MESSAGE_OFFSET, "1"),
                (DeadLetterHeader.SUMMARY, header_summary),
                (DeadLetterHeader.DETAIL, header_detail),
            ],
        )
