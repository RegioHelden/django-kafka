import os
import time

import factory
from factory.django import DjangoModelFactory

from django_kafka.models import WaitingMessage
from django_kafka.utils.message import MessageTimestamp
from example.models import Order


class WaitingMessageFactory(DjangoModelFactory):
    class Meta:
        model = WaitingMessage

    key = factory.LazyFunction(lambda: os.urandom(16))
    value = factory.LazyFunction(lambda: os.urandom(16))
    timestamp = factory.LazyAttribute(
        lambda o: (MessageTimestamp.CREATE_TIME.value, int(time.time() * 1000)),
    )
    topic = factory.Faker("uuid4")
    partition = 1
    offset = 1000


class OrderFactory(DjangoModelFactory):
    class Meta:
        model = Order
