from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

from django.utils import timezone

if TYPE_CHECKING:
    from django_kafka.topic import TopicConsumer


class RetrySettings:
    def __init__(
        self,
        max_retries: int,
        delay: int,
        backoff: bool = False,
        include: Optional[list[type[Exception]]] = None,
        exclude: Optional[list[type[Exception]]] = None,
        blocking: bool = True,
    ):
        """
        :param max_retries: maximum number of retry attempts (use -1 for infinite)
        :param delay: delay (seconds)
        :param backoff: exponential backoff
        :param include: exception types to retry for
        :param exclude: exception types to exclude from retry
        :param blocking: block consumer process during retry
        """
        if max_retries < -1:
            raise ValueError("max_retries must be greater than -1")
        if delay <= 0:
            raise ValueError("delay must be greater than zero")
        if include is not None and exclude is not None:
            raise ValueError("cannot specify both include and exclude")

        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.include = include
        self.exclude = exclude
        self.blocking = blocking

    def __call__(self, topic_cls: type["TopicConsumer"]):
        topic_cls.retry_settings = self
        return topic_cls

    def attempts_exceeded(self, attempt):
        if self.max_retries == -1:
            return False
        return attempt > self.max_retries

    def can_retry(self, attempt: int, exc: Exception) -> bool:
        if self.attempts_exceeded(attempt):
            return False
        if self.include is not None:
            return any(isinstance(exc, e) for e in self.include)
        if self.exclude is not None:
            return not any(isinstance(exc, e) for e in self.exclude)
        return True

    def get_retry_delay(self, attempt: int) -> int:
        return self.delay * (2 ** (attempt - 1)) if self.backoff else self.delay

    def get_retry_time(self, attempt: int) -> datetime:
        return timezone.now() + timedelta(seconds=self.get_retry_delay(attempt))
