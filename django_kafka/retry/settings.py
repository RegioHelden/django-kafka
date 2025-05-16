from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from django.utils import timezone

if TYPE_CHECKING:
    from django_kafka.topic import TopicConsumer


class RetrySettings:
    relational_errors = (
        ObjectDoesNotExist,
        IntegrityError,
    )

    def __init__(
        self,
        max_retries: int,
        delay: int,
        backoff: bool = False,
        include: list[type[Exception]] | None = None,
        exclude: list[type[Exception]] | None = None,
        blocking: bool = True,
        log_every: int | None = None,
        use_offset_tracker: bool = False,
    ):
        """
        :param max_retries: maximum number of retry attempts (use -1 for infinite)
        :param delay: delay (seconds)
        :param backoff: use an exponential backoff delay
        :param include: exception types to retry for
        :param exclude: exception types to exclude from retry
        :param blocking: block the consumer process during retry
        :param log_every: log every Nth retry attempt, default is not to log
        :param use_offset_tracker: use the offset tracker to skip failing messages
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
        self.log_every = log_every
        self.use_offset_tracker = use_offset_tracker

    def __call__(self, topic_cls: type["TopicConsumer"]):
        topic_cls.retry_settings = self
        return topic_cls

    def attempts_exceeded(self, attempt):
        if self.max_retries == -1:
            return False
        return attempt > self.max_retries

    def skip_by_offset(self, msg, exc: Exception):
        # avoiding import errors
        from django_kafka.models import KeyOffsetTracker  # Apps aren't loaded yet

        return all(
            [
                self.use_offset_tracker,
                isinstance(exc, self.relational_errors),
                KeyOffsetTracker.objects.has_future_offset(msg),
            ],
        )

    def should_retry(self, msg, attempt: int, exc: Exception) -> bool:
        if self.skip_by_offset(msg, exc):
            return False
        if self.attempts_exceeded(attempt):
            return False
        if self.include is not None:
            return any(isinstance(exc, e) for e in self.include)
        if self.exclude is not None:
            return not any(isinstance(exc, e) for e in self.exclude)
        return True

    def should_log(self, attempt: int):
        """returns if a message attempt failure should be logged"""
        if self.log_every is None:
            return False
        return attempt % self.log_every == 0

    def get_retry_delay(self, attempt: int) -> int:
        return self.delay * (2 ** (attempt - 1)) if self.backoff else self.delay

    def get_retry_time(self, attempt: int) -> datetime:
        return timezone.now() + timedelta(seconds=self.get_retry_delay(attempt))
