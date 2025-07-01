import os
from datetime import UTC

import factory
from factory.django import DjangoModelFactory

from django_kafka.models import WaitingMessage
from example.models import Order


class WaitingMessageFactory(DjangoModelFactory):
    class Meta:
        model = WaitingMessage

    key = factory.LazyFunction(lambda: os.urandom(16))
    value = factory.LazyFunction(lambda: os.urandom(16))
    timestamp = factory.Faker("date_time", tzinfo=UTC)
    topic = factory.Faker("uuid4")
    partition = 1
    offset = 1000


class OrderFactory(DjangoModelFactory):
    class Meta:
        model = Order
