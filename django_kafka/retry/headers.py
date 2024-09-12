from datetime import datetime
from enum import StrEnum
from typing import Optional

from django.utils import timezone


class RetryHeader(StrEnum):
    MESSAGE = "RETRY_MESSAGE"
    TIMESTAMP = "RETRY_TIMESTAMP"

    @staticmethod
    def get_header(headers: list[tuple], header: "RetryHeader") -> Optional[str]:
        return next((v for k, v in headers if k == header), None)

    @staticmethod
    def get_retry_time(headers: Optional[list[tuple]]) -> Optional["datetime"]:
        """returns the retry time from the message headers"""
        if not headers:
            return None

        header = RetryHeader.get_header(headers, RetryHeader.TIMESTAMP)
        try:
            epoch = float(header)
        except (TypeError, ValueError):
            return None
        else:
            return datetime.fromtimestamp(epoch, tz=timezone.get_current_timezone())
