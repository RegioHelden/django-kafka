from django.db import models
from django.utils.translation import gettext_lazy as _

from django_kafka.producer import Suppression


class KafkaConnectSkipQueryset(models.QuerySet):
    def bulk_create(self, objs, *args, **kwargs):
        if Suppression.active():
            for obj in objs:
                obj.kafka_skip = True

        return super().bulk_create(objs, *args, **kwargs)

    def update(self, **kwargs) -> int:
        if Suppression.active():
            kwargs["kafka_skip"] = True
        else:
            kwargs.setdefault("kafka_skip", False)

        return super().update(**kwargs)

    def delete(self, kafka_skip: bool | None = None):
        if Suppression.active():
            self.filter(kafka_skip=False).update(kafka_skip=True)
        elif kafka_skip is not None:
            self.filter(kafka_skip=not kafka_skip).update(kafka_skip=kafka_skip)
        else:
            self.filter(kafka_skip=True).update(kafka_skip=False)

        return super().delete()


class KafkaConnectSkipModel(models.Model):
    """
    For models (tables) which have Kafka Connect source connectors attached and require
    a flag to suppress message production.

    The Kafka Connect connector should filter out events based on the kafka_skip flag
    provided in this model.

    Any update to the model instance will reset the kafka_skip flag to False, if not
    explicitly set.

    This flag can help overcome infinite event loops during bidirectional data sync when
    using Kafka. See README.md for more information.
    """

    kafka_skip = models.BooleanField(
        _("Kafka skip"),
        help_text=_(
            "Used by Kafka Connect to suppress event creation."
            "\nGets reset to False on .save() method call, unless explicitly set.",
        ),
        default=False,
        editable=False,
    )
    objects = KafkaConnectSkipQueryset.as_manager()

    class Meta:
        abstract = True
        base_manager_name = "objects"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._reset_kafka_skip = False  # set True for DB fetched instances in from_db

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key == "kafka_skip":
            self._reset_kafka_skip = False

    def save(
        self,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        if Suppression.active():
            # Global producer suppression is enabled. Set True only if necessary.
            if not self.kafka_skip:
                self.kafka_skip = True
                update_fields = self.__add_skip_to_update_fields(update_fields)
        elif self._reset_kafka_skip:
            # kafka_skip was not manually set. Set False only if necessary.
            if self.kafka_skip:
                self.kafka_skip = False
                update_fields = self.__add_skip_to_update_fields(update_fields)
        elif not self._reset_kafka_skip:
            # kafka_skip was manually set, so add it to update fields.
            update_fields = self.__add_skip_to_update_fields(update_fields)

        ret = super().save(
            force_insert=force_insert,
            force_update=force_update,
            using=using,
            update_fields=update_fields,
        )
        self._reset_kafka_skip = True

        return ret

    def delete(self, *args, **kwargs):
        if self.pk is not None:
            # kafka_skip must be correctly set in the DB before performing delete.
            if Suppression.active():
                # Global producer suppression is enabled. Set True only if necessary.
                if not self.kafka_skip:
                    self.kafka_skip = True
                    self.save(update_fields=["kafka_skip"])
            elif self._reset_kafka_skip:
                # kafka_skip was not manually set. Set False only if necessary.
                if self.kafka_skip:
                    self.kafka_skip = False
                    self.save(update_fields=["kafka_skip"])
            elif not self._reset_kafka_skip:
                # kafka skip was manually set, so save it before delete.
                self.save(update_fields=["kafka_skip"])

        ret = super().delete(*args, **kwargs)
        self._reset_kafka_skip = True

        return ret

    @classmethod
    def __add_skip_to_update_fields(cls, update_fields):
        if update_fields and "kafka_skip" not in update_fields:
            return ["kafka_skip", *update_fields]
        return update_fields

    @classmethod
    def from_db(cls, *args, **kwargs):
        """
        Used by Django's QuerySet to initialize instances fetched from db, and won't
        be called by QuerySet.create or direct initialization.

        The kafka_skip value stored in the DB should not be reused; instead it should be
        reset to False before updating - unless explicitly set.
        """
        instance = super().from_db(*args, **kwargs)
        instance._reset_kafka_skip = True
        return instance

    def refresh_from_db(self, *args, **kwargs):
        ret = super().refresh_from_db(*args, **kwargs)
        self._reset_kafka_skip = True
        return ret
