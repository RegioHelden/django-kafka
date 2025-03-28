from django.contrib import admin

from django_kafka.models import KeyOffsetTracker

admin.site.register(KeyOffsetTracker)
