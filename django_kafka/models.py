from django.db import models
from django.utils.translation import gettext_lazy as _


class KafkaSkipQueryset(models.QuerySet):
    def update(self, **kwargs) -> int:
        kwargs.setdefault("kafka_skip", False)
        return super().update(**kwargs)


class KafkaSkipMixin(models.Model):
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

    def save(self, **kwargs):
        if "update_fields" not in kwargs:
            self.kafka_skip = False

        elif "kafka_skip" not in kwargs["update_fields"]:
            self.kafka_skip = False
            kwargs["update_fields"] = ["kafka_skip", *kwargs["update_fields"]]

        super().save(**kwargs)
