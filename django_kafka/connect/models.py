from django.db import models
from django.utils.translation import gettext_lazy as _


class KafkaConnectSkipQueryset(models.QuerySet):
    def update(self, **kwargs) -> int:
        kwargs.setdefault("kafka_skip", False)
        return super().update(**kwargs)


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
        if self._reset_kafka_skip:
            self.kafka_skip = False
            if update_fields and "kafka_skip" not in update_fields:
                update_fields = ["kafka_skip", *update_fields]

        super().save(
            force_insert=force_insert,
            force_update=force_update,
            using=using,
            update_fields=update_fields,
        )
        self._reset_kafka_skip = True

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
        super().refresh_from_db(*args, **kwargs)
        self._reset_kafka_skip = True
