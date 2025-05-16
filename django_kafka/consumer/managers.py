from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING

from confluent_kafka import TopicPartition
from django.utils import timezone

if TYPE_CHECKING:
    from confluent_kafka import cimpl


class PauseManager:
    """Manager for partition pauses"""

    __pauses: dict[TopicPartition, datetime]

    def __init__(self):
        self.__pauses = {}

    @staticmethod
    def get_msg_partition(msg: "cimpl.Message") -> TopicPartition:
        """returns the message topic partition"""
        return TopicPartition(msg.topic(), msg.partition(), msg.offset())

    def set(self, msg: "cimpl.Message", until: datetime) -> TopicPartition:
        """adds message partition to the pause list, returning the partition"""
        tp = self.get_msg_partition(msg)
        self.__pauses[tp] = until
        return tp

    def pop_ready(self) -> Iterator[TopicPartition]:
        """returns the partitions ready to resume, removing them from the pause list"""
        now = timezone.now()
        for tp, pause in list(self.__pauses.items()):
            if now >= pause:
                del self.__pauses[tp]
                yield tp

    def reset(self):
        self.__pauses = {}


class RetryManager:
    """Manager for blocking message retry attempts"""

    __retries: dict[TopicPartition, int]

    def __init__(self):
        self.__retries = {}

    @staticmethod
    def get_msg_partition(msg: "cimpl.Message") -> TopicPartition:
        """returns the message topic partition, set to the message offset

        Note: TopicPartition hashes based on topic/partition, but not the offset.
        """
        return TopicPartition(msg.topic(), msg.partition(), msg.offset())

    def next(self, msg: "cimpl.Message"):
        """increments and returns the partition attempt count"""
        msg_tp = self.get_msg_partition(msg)
        for tp in self.__retries:
            if tp == msg_tp and tp.offset != msg_tp.offset:
                del self.__retries[tp]  # new offset encountered, reset entry
                break

        next_attempt = self.__retries.get(msg_tp, 0) + 1
        self.__retries[msg_tp] = next_attempt
        return next_attempt

    def reset(self):
        self.__retries = {}
