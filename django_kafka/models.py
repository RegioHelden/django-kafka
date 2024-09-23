from django.db import models
from django.utils.translation import gettext_lazy as _


class KafkaSkipQueryset(models.QuerySet):
    def update(self, **kwargs) -> int:
        kwargs.setdefault("kafka_skip", False)
        return super().update(**kwargs)


class KafkaSkipModel(models.Model):
    """
    For models (tables) which are synced with other database(s) in both directions.

    Every update which happens from within the system should set `kafka_skip=False`,
     global producer (kafka connect, django post_save signal, etc.) will then create
     a new event.

    When db update comes from the consumed event, then the row should be manually
     marked for skip `kafka_skip=True`, and kafka connector or global python producer
     should not generate a new one by filtering it out based on `kafka_skip` field.
    """

    kafka_skip = models.BooleanField(
        _("Kafka skip"),
        help_text=_(
            "Wont generate an event if `True`."
            "\nThis field is used to filter out the events to break the infinite loop"
            " of message generation when synchronizing 2+ databases."
            "\nGets reset to False on .save() method call.",
        ),
        default=False,
    )
    objects = KafkaSkipQueryset.as_manager()

    class Meta:
        abstract = True
        base_manager_name = "objects"

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._reset_kafka_skip = False  # set True for DB fetched instances in from_db

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key == "kafka_skip":
            self._reset_kafka_skip = False

    def refresh_from_db(self, *args, **kwargs):
        super().refresh_from_db(*args, **kwargs)
        self._reset_kafka_skip = True
