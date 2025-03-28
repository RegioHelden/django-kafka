from django.contrib import admin

from django_kafka.models import KeyOffsetTracker


@admin.register(KeyOffsetTracker)
class KeyOffsetTrackerAdmin(admin.ModelAdmin):
    list_display = ("topic", "key", "offset")
    search_fields = ("topic", "key")
