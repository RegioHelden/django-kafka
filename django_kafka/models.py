from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, ClassVar

from django.db import models
from django.utils.translation import gettext_lazy as _

from django_kafka.relations_resolver.relation import RelationType
from django_kafka.utils.message import MessageTimestamp

if TYPE_CHECKING:
    from confluent_kafka import cimpl

    from django_kafka.relations_resolver.relation import ModelRelation


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
    def timestamp(self, value: tuple[MessageTimestamp, int]):
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


class WaitingMessageQuerySet(models.QuerySet):
    def add_message(self, msg: "cimpl.Message", relation: "ModelRelation"):
        from django_kafka.relations_resolver.relation import ModelRelation

        return self.create(
            key=msg.key(),
            value=msg.value(),
            timestamp=msg.timestamp(),
            topic=msg.topic(),
            partition=msg.partition(),
            headers=msg.headers(),
            offset=msg.offset(),
            relation_model_key=ModelRelation.get_model_key(relation.model),
            relation_id_field=relation.id_field,
            relation_id_value=relation.id_value,
            serialized_relation=relation.serialize(),
        )

    def for_relation(self, relation):
        from django_kafka.relations_resolver.relation import ModelRelation

        return self.filter(
            relation_model_key=ModelRelation.get_model_key(relation.model),
            relation_id_field=relation.id_field,
            relation_id_value=relation.id_value,
        ).order_by("topic", "partition", "offset")

    def relations(self):
        return (
            self.order_by(
                "relation_model_key",
                "relation_id_field",
                "relation_id_value",
            )
            .distinct("relation_model_key", "relation_id_field", "relation_id_value")
            .only("relation_model_key", "relation_id_field", "relation_id_value")
        )

    def waiting(self):
        return self.filter(status=self.model.Status.WAITING)

    def mark_resolving(self, relation):
        self.for_relation(relation).update(status=self.model.Status.RESOLVING)

    async def aiter_relations_to_resolve(
        self,
        chunk_size=500,
    ) -> AsyncIterator["WaitingMessage"]:
        async for relation in (
            self.waiting().relations().aiterator(chunk_size=chunk_size)
        ):
            yield relation


class WaitingMessage(models.Model):
    class Status(models.IntegerChoices):
        WAITING = 1
        RESOLVING = 2
        RESOLVED = 3

    status = models.IntegerField(
        _("status"),
        choices=Status.choices,
        default=Status.WAITING,
    )

    key = models.BinaryField(_("key"))
    value = models.BinaryField(_("value"))
    timestamp = models.JSONField(_("kafka timestamp"))
    topic = models.TextField(_("topic"))
    partition = models.TextField(_("partition"))
    offset = models.TextField(_("offset"))
    _headers = models.JSONField(_("headers"), null=True)

    relation_model_key = models.CharField(_("relation model key"), max_length=255)
    relation_id_field = models.CharField(_("relation id field"), max_length=256)
    relation_id_value = models.CharField(_("relation id value"), max_length=256)

    serialized_relation = models.JSONField(_("relation kwargs"))

    objects = WaitingMessageQuerySet.as_manager()

    class Meta:
        verbose_name = _("waiting message")
        verbose_name_plural = _("waiting messages")
        ordering = ("offset",)

    def __str__(self):
        return (
            f"{self.relation_model_key}"
            f"({self.relation_id_field}={self.relation_id_value})"
        )

    @property
    def headers(self) -> list[tuple[str, bytes]] | None:
        if not self._headers:
            return None
        return [(k, bytes(v) if isinstance(v, list) else v) for k, v in self._headers]

    @headers.setter
    def headers(self, value: list[tuple[str, bytes]]):
        if not value:
            self._headers = None
        else:
            self._headers = [
                (k, list(v) if isinstance(v, bytes) else v) for k, v in value
            ]

    def relation(self):
        return RelationType.instance(self.serialized_relation)
