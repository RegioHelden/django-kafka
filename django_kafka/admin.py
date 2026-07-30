from django.contrib import admin

from django_kafka.models import WaitingMessage


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
    list_filter = ("status",)
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
