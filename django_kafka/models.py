from typing import ClassVar

from django.db import models
from django.utils.translation import gettext_lazy as _

from django_kafka.utils.message import MessageTimestamp


class KeyOffsetTrackerQuerySet(models.QuerySet):
    def log_msg_offset(self, msg) -> tuple["KeyOffsetTracker", bool]:
        return self.update_or_create(
            topic=msg.topic(),
            key=msg.key(),
            defaults={
                "offset": msg.offset(),
                "timestamp": msg.timestamp(),
            },
        )

    def has_future_offset(self, msg) -> bool:
        return self.filter(
            topic=msg.topic(),
            key=msg.key(),
            offset__gt=msg.offset(),
        ).exists()


class KeyOffsetTracker(models.Model):
    topic = models.CharField(_("topic"), max_length=256)
    key = models.BinaryField(_("key"))
    offset = models.PositiveIntegerField(_("offset"))
    create_time = models.DateTimeField(_("create time"), null=True)
    log_append_time = models.DateTimeField(_("log append time"), null=True)

    objects = KeyOffsetTrackerQuerySet.as_manager()

    class Meta:
        verbose_name = _("key offset tracker")
        verbose_name_plural = _("key offsets tracker")
        indexes: ClassVar = [
            models.Index(
                fields=["topic", "key"],
                include=["offset"],
                name="%(app_label)s_key_offset_index",
            ),
        ]

    def __str__(self):
        return f"{self.topic}:{self.key}:{self.offset}"

    @property
    def timestamp(self):
        if self.log_append_time:
            return self.log_append_time
        return self.create_time or self.log_append_time

    @timestamp.setter
    def timestamp(self, value: [MessageTimestamp, int]):
        if not value:
            return

        timestamp_type = value[0]

        if timestamp_type == MessageTimestamp.CREATE_TIME:
            self.create_time = MessageTimestamp.to_datetime(value)
            self.log_append_time = None

        elif timestamp_type == MessageTimestamp.LOG_APPEND_TIME:
            self.create_time = None
            self.log_append_time = MessageTimestamp.to_datetime(value)

        else:
            self.create_time = None
            self.log_append_time = None
