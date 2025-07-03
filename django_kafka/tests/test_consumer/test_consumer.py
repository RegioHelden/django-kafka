import datetime
import traceback
from contextlib import suppress
from typing import ClassVar
from unittest.mock import MagicMock, Mock, call, patch

from confluent_kafka import TopicPartition
from django.test import TestCase, override_settings
from django.utils import timezone

from django_kafka.conf import SETTINGS_KEY, settings
from django_kafka.consumer import Consumer, Topics
from django_kafka.relations_resolver.resolver import RelationResolver
from django_kafka.retry.settings import RetrySettings
from django_kafka.tests.utils import message_mock


class StopWhileTrue:
    class Error(Exception):
        pass

    def __init__(self, stop_on):
        self.stop_on = stop_on

    def __call__(self, arg):
        if arg == self.stop_on:
            raise self.Error


class ConsumerTestCase(TestCase):
    def test_group_id(self):
        class SomeConsumer(Consumer):
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()

        self.assertEqual(consumer.group_id, "group_id")

    @patch(
        "django_kafka.consumer.Consumer.process_message",
        side_effect=StopWhileTrue(stop_on=False),
    )
    @patch(
        "django_kafka.consumer.consumer.ConfluentConsumer",
        **{
            # first iteration - no message (None)
            # second iteration - 1 non-empty message represented as True for testing
            # third iteration - 1 empty message represented as False for testing
            "return_value.poll.side_effect": [None, True, False],
        },
    )
    def test_run(self, mock_consumer_client, mock_process_message):
        class SomeConsumer(Consumer):
            topics = MagicMock()
            log_error = Mock()

        consumer = SomeConsumer()
        consumer.resume_partitions = Mock()

        # hack to break infinite loop
        # `consumer.start` is using `while True:` loop which never ends
        # in order to test it we need to break it which is achievable with side_effect
        with suppress(StopWhileTrue.Error):
            consumer.run()

        # assert partitions were resumed for each poll
        self.assertEqual(consumer.resume_partitions.call_count, 3)

        # subscribed to the topics defined by consumer class
        mock_consumer_client.return_value.subscribe.assert_called_once_with(
            topics=consumer.topics.names,
        )
        # poll for message with the frequency defined by consumer class
        self.assertEqual(
            mock_consumer_client.return_value.poll.call_args_list,
            # 3 calls expected as we setup 3 iterations in the test
            [call(timeout=consumer.polling_freq)] * 3,
        )
        # process_message called twice as None message is ignored
        self.assertEqual(mock_process_message.call_args_list, [call(True), call(False)])

    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_pause_partition(self, mock_confluent_consumer):
        class SomeConsumer(Consumer):
            topics = MagicMock()

        consumer = SomeConsumer()
        mock_msg = message_mock()
        partition = TopicPartition(
            mock_msg.topic(),
            mock_msg.partition(),
            mock_msg.offset(),
        )
        consumer._pauses.set = Mock(return_value=partition)
        retry_time = timezone.now()

        consumer.pause_partition(mock_msg, retry_time)

        consumer._pauses.set.assert_called_once_with(mock_msg, retry_time)
        mock_confluent_consumer.return_value.seek.assert_called_once_with(partition)
        pause = mock_confluent_consumer.return_value.pause
        pause.assert_called_once_with([partition])
        self.assertEqual(pause.call_args.args[0][0].offset, mock_msg.offset())

    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_resume_partitions__before_time(self, mock_confluent_consumer):
        class SomeConsumer(Consumer):
            topics = MagicMock()

        consumer = SomeConsumer()
        mock_msg = message_mock()
        retry_time = timezone.now() + datetime.timedelta(minutes=1)

        consumer.pause_partition(mock_msg, retry_time)
        consumer.resume_partitions()

        mock_confluent_consumer.return_value.resume.assert_not_called()

    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_resume_partitions__after_time(self, mock_confluent_consumer):
        class SomeConsumer(Consumer):
            topics = MagicMock()

        consumer = SomeConsumer()
        mock_msg = message_mock()
        partition = TopicPartition(
            mock_msg.topic(),
            mock_msg.partition(),
            mock_msg.offset(),
        )
        retry_time = timezone.now() - datetime.timedelta(minutes=1)

        consumer.pause_partition(mock_msg, retry_time)
        consumer.resume_partitions()

        resume = mock_confluent_consumer.return_value.resume
        resume.assert_called_once_with([partition])
        self.assertEqual(resume.call_args.args[0][0].offset, mock_msg.offset())

    @patch("django_kafka.consumer.Consumer.commit_offset")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message_success(self, mock_consumer_client, mock_commit_offset):
        class SomeConsumer(Consumer):
            topics = MagicMock()

        msg = message_mock()
        consumer = SomeConsumer()

        consumer.process_message(msg)

        # checks msg had error before processing
        msg.error.assert_called_once_with()
        # Topic.consume called
        consumer.get_topic(msg).consume.assert_called_once_with(msg)
        # commit_offset triggered
        mock_commit_offset.assert_called_once_with(msg)

    @patch("django_kafka.consumer.Consumer.log_error")
    @patch("django_kafka.consumer.Consumer.commit_offset")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message_msg_error_logged(
        self,
        mock_consumer_client,
        mock_commit_offset,
        log_error,
    ):
        class SomeConsumer(Consumer):
            topics = MagicMock()

        msg = message_mock(error=True)
        consumer = SomeConsumer()

        consumer.process_message(msg)

        # checks msg had error before processing
        msg.error.assert_called_once_with()
        # error handler was triggered
        log_error.assert_called_once_with(msg)
        # Topic.consume is not called
        consumer.topics[msg.topic()].consume.assert_not_called()
        # Consumer.commit_offset is not called
        mock_commit_offset.assert_not_called()

    @patch("django_kafka.consumer.Consumer.handle_exception", return_value=True)
    @patch("django_kafka.consumer.Consumer.commit_offset")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message__processed_exception(
        self,
        mock_consumer_client,
        mock_commit_offset,
        mock_handle_exception,
    ):
        topic_consume_side_effect = TypeError("test")
        topic_consumer = Mock(
            **{
                "consume.side_effect": topic_consume_side_effect,
                "use_relations_resolver": False,
            },
        )
        topic_consumer.name = "topic"
        msg = message_mock(topic_consumer.name)

        class SomeConsumer(Consumer):
            topics = Topics(topic_consumer)

        consumer = SomeConsumer()

        consumer.process_message(msg)

        mock_handle_exception.assert_called_once_with(msg, topic_consume_side_effect)
        # Consumer.commit_offset is called
        mock_commit_offset.assert_called_once()

    @patch("django_kafka.consumer.Consumer.handle_exception", return_value=False)
    @patch("django_kafka.consumer.Consumer.commit_offset")
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_process_message__unprocessed_exception(
        self,
        mock_consumer_client,
        mock_commit_offset,
        mock_handle_exception,
    ):
        topic_consume_side_effect = TypeError("test")
        topic_consumer = Mock(
            **{
                "consume.side_effect": topic_consume_side_effect,
                "use_relations_resolver": False,
            },
        )
        topic_consumer.name = "topic"
        msg = message_mock(topic_consumer.name)

        class SomeConsumer(Consumer):
            topics = Topics(topic_consumer)

        consumer = SomeConsumer()

        consumer.process_message(msg)

        mock_handle_exception.assert_called_once_with(msg, topic_consume_side_effect)
        # Consumer.commit_offset was not called
        mock_commit_offset.assert_not_called()

    def test_handle_exception(self):
        msg = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}
            retry_msg = Mock(return_value=(True, False))  # successful retry
            dead_letter_msg = Mock()
            log_error = Mock()

        consumer = SomeConsumer()
        exc = ValueError()

        consumer.handle_exception(msg, exc)

        consumer.retry_msg.assert_called_once_with(msg, exc)
        consumer.dead_letter_msg.assert_not_called()
        consumer.log_error.assert_not_called()

    def test_handle_exception__failed_retry(self):
        msg = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}
            retry_msg = Mock(return_value=(False, False))  # failed retry
            dead_letter_msg = Mock()
            log_error = Mock()

        consumer = SomeConsumer()
        exc = ValueError()

        consumer.handle_exception(msg, exc)

        consumer.retry_msg.assert_called_once_with(msg, exc)
        consumer.dead_letter_msg.assert_called_once_with(msg, exc)
        consumer.log_error.assert_called_once_with(msg, exc_info=exc)

    def test_blocking_retry(self):
        retry_time = timezone.now()
        retry_settings = RetrySettings(
            max_retries=5,
            delay=60,
            blocking=True,
            log_every=1,
        )
        retry_settings.get_retry_time = Mock(return_value=retry_time)
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        consumer.pause_partition = Mock()
        consumer.log_error = Mock()
        exc = ValueError()

        retried = consumer.blocking_retry(retry_settings, msg_mock, exc)

        consumer.pause_partition.assert_called_once_with(msg_mock, retry_time)
        consumer.log_error.assert_called_once_with(msg_mock, exc_info=exc)
        self.assertEqual(retried, True)

    @patch("django_kafka.consumer.Consumer.log_error")
    def test_blocking_retry__maximum_attempts(self, log_error):
        retry_settings = RetrySettings(
            max_retries=2,
            delay=60,
            blocking=True,
            log_every=2,
        )
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        consumer.pause_partition = Mock()
        consumer.log_error = Mock()
        exc = ValueError()

        retry_1 = consumer.blocking_retry(retry_settings, msg_mock, exc)
        retry_2 = consumer.blocking_retry(retry_settings, msg_mock, exc)
        retry_3 = consumer.blocking_retry(retry_settings, msg_mock, exc)

        self.assertTrue(retry_1)
        self.assertTrue(retry_2)
        self.assertFalse(retry_3)
        self.assertEqual(consumer.pause_partition.call_count, 2)
        self.assertEqual(consumer.log_error.call_count, 1)

    @patch("django_kafka.retry.topic.RetryTopicProducer")
    def test_non_blocking_retry(self, mock_rt_producer_cls):
        mock_retry = mock_rt_producer_cls.return_value.retry
        retry_settings = RetrySettings(max_retries=5, delay=60, blocking=False)
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        exc = ValueError()

        retried = consumer.non_blocking_retry(
            retry_settings,
            msg_mock,
            exc,
        )

        mock_rt_producer_cls.assert_called_once_with(
            retry_settings=retry_settings,
            group_id=consumer.group_id,
            msg=msg_mock,
        )
        mock_retry.assert_called_once_with(exc=exc)
        self.assertEqual(retried, mock_retry.return_value)

    def test_retry__respects_blocking(self):
        mock_topic_consumer = Mock(**{"retry_settings.blocking": True})
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        consumer.blocking_retry = Mock()
        consumer.non_blocking_retry = Mock()
        consumer.get_topic = Mock(return_value=mock_topic_consumer)
        exc = ValueError()

        retried, blocking = consumer.retry_msg(msg_mock, exc)

        consumer.non_blocking_retry.assert_not_called()
        consumer.blocking_retry.assert_called_once_with(
            mock_topic_consumer.retry_settings,
            msg_mock,
            exc,
        )
        self.assertEqual(retried, consumer.blocking_retry.return_value)
        self.assertEqual(blocking, True)

    def test_retry__respects_non_blocking(self):
        mock_topic_consumer = Mock(**{"retry_settings.blocking": False})
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        consumer.blocking_retry = Mock()
        consumer.non_blocking_retry = Mock()
        consumer.get_topic = Mock(return_value=mock_topic_consumer)
        exc = ValueError()

        retried, blocking = consumer.retry_msg(msg_mock, exc)

        consumer.blocking_retry.assert_not_called()
        consumer.non_blocking_retry.assert_called_once_with(
            mock_topic_consumer.retry_settings,
            msg_mock,
            exc,
        )
        self.assertEqual(retried, consumer.non_blocking_retry.return_value)
        self.assertEqual(blocking, False)

    def test_retry_msg__no_retry(self):
        mock_topic_consumer = Mock(retry_settings=None)
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        consumer.blocking_retry = Mock()
        consumer.non_blocking_retry = Mock()
        consumer.get_topic = Mock(return_value=mock_topic_consumer)
        exc = ValueError()

        retried, blocking = consumer.retry_msg(msg_mock, exc)

        consumer.blocking_retry.assert_not_called()
        consumer.non_blocking_retry.assert_not_called()
        self.assertEqual(retried, False)
        self.assertEqual(blocking, False)

    @patch("django_kafka.dead_letter.topic.DeadLetterTopicProducer")
    def test_dead_letter_msg(self, mock_dead_letter_topic_cls):
        mock_produce_for = mock_dead_letter_topic_cls.return_value.produce_for
        msg_mock = message_mock()

        class SomeConsumer(Consumer):
            topics = Topics()
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        exc = ValueError()

        consumer.dead_letter_msg(msg_mock, exc)

        mock_dead_letter_topic_cls.assert_called_once_with(
            group_id=consumer.group_id,
            msg=msg_mock,
        )
        mock_produce_for.assert_called_once_with(
            header_summary=str(exc),
            header_detail=traceback.format_exc(),
        )

    @patch("django_kafka.consumer.consumer.ConfluentConsumer", new=Mock())
    def test_use_relation_resolver(self):
        class SomeConsumer(Consumer):
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()

        topic_consumer_a = Mock(use_relations_resolver=True)
        msg_a = message_mock()
        with (
            patch(
                "django_kafka.consumer.consumer.Consumer.get_topic",
                return_value=topic_consumer_a,
            ),
            patch(
                "django_kafka.consumer.consumer.Consumer._resolve_relations",
            ) as mock_resolve_relations,
        ):
            consumer.consume(msg_a)
            mock_resolve_relations.assert_called_once_with(msg_a, topic_consumer_a)
            topic_consumer_a.consume.assert_not_called()

        topic_consumer_b = Mock(use_relations_resolver=False)
        msg_b = message_mock()
        with (
            patch(
                "django_kafka.consumer.consumer.Consumer.get_topic",
                return_value=topic_consumer_b,
            ),
            patch(
                "django_kafka.consumer.consumer.Consumer._resolve_relations",
            ) as mock_resolve_relations,
        ):
            consumer.consume(msg_b)
            mock_resolve_relations.assert_not_called()
            topic_consumer_b.consume.assert_called_once_with(msg_b)

    @patch("django_kafka.consumer.consumer.ConfluentConsumer", new=Mock())
    def test_resolve_relations_return_value(self):
        class SomeConsumer(Consumer):
            config: ClassVar = {"group.id": "group_id"}

        consumer = SomeConsumer()
        topic = Mock()
        msg = message_mock()

        with patch(
            "django_kafka.kafka.relations_resolver.resolve",
            return_value=RelationResolver.Action.CONTINUE,
        ):
            self.assertTrue(consumer._resolve_relations(msg, topic))

        with patch(
            "django_kafka.kafka.relations_resolver.resolve",
            return_value=RelationResolver.Action.SKIP,
        ):
            self.assertTrue(consumer._resolve_relations(msg, topic))

        with patch(
            "django_kafka.kafka.relations_resolver.resolve",
            return_value=RelationResolver.Action.PAUSE,
        ):
            self.assertFalse(consumer._resolve_relations(msg, topic))

    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_auto_offset_false(self, mock_consumer_client):
        class SomeConsumer(Consumer):
            config: ClassVar = {"enable.auto.offset.store": False}

        consumer = SomeConsumer()
        msg = message_mock()

        consumer.commit_offset(msg)

        mock_consumer_client.return_value.store_offsets.assert_called_once_with(msg)

    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    def test_auto_offset_true(self, mock_consumer_client):
        class SomeConsumer(Consumer):
            config: ClassVar = {"enable.auto.offset.store": True}

        consumer = SomeConsumer()

        consumer.commit_offset(Mock())

        mock_consumer_client.return_value.store_offsets.assert_not_called()

    def test_settings_are_correctly_assigned(self):
        self.assertEqual(Consumer.polling_freq, settings.POLLING_FREQUENCY)
        self.assertEqual(Consumer.default_error_handler, settings.ERROR_HANDLER)

    @override_settings(
        **{
            SETTINGS_KEY: {
                "CLIENT_ID": "client.id-initial-definition",
                "GLOBAL_CONFIG": {
                    "client.id": "client-id-overridden-by-global-config",
                    "bootstrap.servers": "defined-in-global-config",
                },
                "CONSUMER_CONFIG": {
                    "bootstrap.servers": "bootstrap.servers-overridden-by-consumer",
                    "group.id": "group.id-defined-by-consumer-config",
                },
                "ERROR_HANDLER": "django_kafka.error_handlers.ClientErrorHandler",
            },
        },
    )
    @patch("django_kafka.consumer.consumer.ConfluentConsumer")
    @patch("django_kafka.error_handlers.ClientErrorHandler")
    def test_config_merge_override(self, mock_error_handler, mock_consumer_client):
        """
        1. CLIENT_ID is added to the consumers config
        2. GLOBAL_CONFIG overrides CLIENT_ID (client.id) if it contains one
        3. CONSUMER_CONFIG merges with GLOBAL_CONFIG and overrides keys if any
        4. Consumer.config is merged with CONSUMER_CONFIG and overrides keys if any
        """

        class SomeConsumer(Consumer):
            config: ClassVar = {
                "group.id": "group.id.overridden-by-consumer-class",
                "enable.auto.offset.store": True,
            }

        # Consumer.config is properly merged
        consumer = SomeConsumer()

        self.assertDictEqual(
            SomeConsumer.build_config(),
            {
                "client.id": "client-id-overridden-by-global-config",
                "bootstrap.servers": "bootstrap.servers-overridden-by-consumer",
                "group.id": "group.id.overridden-by-consumer-class",
                "enable.auto.offset.store": True,
                "logger": SomeConsumer.default_logger,
                "error_cb": mock_error_handler(),
            },
        )
        self.assertDictEqual(
            SomeConsumer.build_config(),
            consumer.config,
        )
