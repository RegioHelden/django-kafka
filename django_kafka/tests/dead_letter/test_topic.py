from unittest import mock

from django.test import TestCase

from django_kafka.dead_letter.headers import DeadLetterHeader
from django_kafka.dead_letter.topic import DeadLetterTopic
from django_kafka.topic import Topic


class DeadLetterTopicTestCase(TestCase):
    def _get_main_topic(self, topic_name: str = "topic_name"):
        class TestTopic(Topic):
            name = topic_name

        return TestTopic()

    def test_name(self):
        main_topic = self._get_main_topic(topic_name="topic.name")
        dl_topic = DeadLetterTopic(group_id="group.id", main_topic=main_topic)

        self.assertEqual(dl_topic.name, r"^group\.id\.topic\.name\.dlt$")

    def test_name__regex(self):
        """tests regex topic names are correctly inserted in to dlt regex"""
        main_topic = self._get_main_topic(topic_name="^topic_name|other_name$")
        dl_topic = DeadLetterTopic(group_id="group.id", main_topic=main_topic)

        self.assertEqual(
            dl_topic.name,
            r"^group\.id\.(topic_name|other_name)\.dlt$",
        )

    def test_get_produce_name(self):
        """tests normal and retry topics are correctly handled by get_produce_name"""
        main_topic = self._get_main_topic()
        dl_topic = DeadLetterTopic(group_id="group.id", main_topic=main_topic)

        retry_topic_1 = dl_topic.get_produce_name("fake")
        retry_topic_2 = dl_topic.get_produce_name("group.id.fake.retry.1")

        self.assertEqual(retry_topic_1, "group.id.fake.dlt")
        self.assertEqual(retry_topic_2, "group.id.fake.dlt")

    @mock.patch("django_kafka.dead_letter.topic.DeadLetterTopic.produce")
    def test_produce_for(self, mock_produce):
        main_topic = self._get_main_topic()
        group_id = "group.id"
        header_message = "header_message"
        header_detail = "header_detail"
        msg_mock = mock.Mock(**{"topic.return_value": "msg_topic"})

        DeadLetterTopic(group_id=group_id, main_topic=main_topic).produce_for(
            msg=msg_mock,
            header_message=header_message,
            header_detail=header_detail,
        )

        mock_produce.assert_called_once_with(
            name="group.id.msg_topic.dlt",
            key=msg_mock.key(),
            value=msg_mock.value(),
            headers=[
                (DeadLetterHeader.MESSAGE, header_message),
                (DeadLetterHeader.DETAIL, header_detail),
            ],
        )
