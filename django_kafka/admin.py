from django.contrib import admin

from django_kafka.models import KeyOffsetTracker, WaitingMessage


@admin.register(KeyOffsetTracker)
class KeyOffsetTrackerAdmin(admin.ModelAdmin):
    list_display = ("topic", "key", "offset")
    search_fields = ("topic", "key")


@admin.register(WaitingMessage)
class WaitingMessageAdmin(admin.ModelAdmin):
    list_display = (
        "topic",
        "partition",
        "offset",
        "relation_model_key",
        "relation_id_field",
        "relation_id_value",
        "status",
    )
    search_fields = (
        "topic",
        "partition",
        "offset",
        "relation_model_key",
        "relation_id_field",
        "relation_id_value",
        "key",
        "status",
    )
