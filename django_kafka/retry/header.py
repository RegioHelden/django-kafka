from datetime import datetime
from typing import Optional

from django.utils import timezone

from django_kafka.utils.message import Header


class RetryHeader(Header):
    MESSAGE = "RETRY_MESSAGE"
    TIMESTAMP = "RETRY_TIMESTAMP"

    @classmethod
    def get_retry_time(cls, headers: list[tuple] | None) -> Optional["datetime"]:
        """returns the retry time from the message headers"""
        header = RetryHeader.get(headers, RetryHeader.TIMESTAMP)
        try:
            epoch = float(header)
        except (TypeError, ValueError):
            return None
        else:
            return datetime.fromtimestamp(epoch, tz=timezone.get_current_timezone())
