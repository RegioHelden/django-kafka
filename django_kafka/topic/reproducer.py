import contextlib
from abc import ABC
from typing import TYPE_CHECKING, Any

from confluent_kafka.serialization import MessageField

from django_kafka.conf import settings
from django_kafka.topic.avro import AvroTopicConsumer

if TYPE_CHECKING:
    from django.db.models import Model


class ReproduceTopic(AvroTopicConsumer, ABC):
    """
    Class to consume messages from a source topic and distribute them to a specific
    topic reproducer class which augments the events with extra data as needed.

    The typical use case is to consume messages from a debezium source connector and
    then add related field data to those messages.
    """

    reproducer: "TopicReproducer"

    @classmethod
    def _is_deletion(cls, value) -> bool:
        return value is None or value.get("__deleted") == "true"

    @classmethod
    def _skip_reproduce(cls, value):
        # necessary to not filter out deletion events for rows marked as kafka_skip=true
        return not cls._is_deletion(value) and value.get("kafka_skip")

    def consume(self, msg):
        key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())

        if not self._skip_reproduce(value):
            self.reproducer.reproduce(key, value, self._is_deletion(value))


class TopicReproducer:
    """
    Simplifies reproducing messages from Django model instance changes or a custom
    topic. The following attributes/methods may be overridden:

    1. `reproduce_model` (optional) - the model class for which messages will be
        reproduced from events to a debezium source connector topic. If not set,
        then `reproduce_name` must be set and `reproduce` overridden.
    2. `reproduce_name` (optional) - the topic name to reproduce messages from, for when
        there is no `reproduce_model` or a custom topic name is required.
    3. `reproduce_namespace` (optional) - if the debezium source connector prepends
        topic names with a namespace, specify this here.
    4. `reproduce` (optional) - defines default reproduce behaviour which calls
        `_reproduce_upsert` and `_reproduce_deletion` depending on instance upsert or
        deletion respectively. These latter two methods must be implemented if this
        method is not overridden.
    """

    reproduce_namespace: str | None = settings.REPRODUCER_CONFIG["namespace"]
    reproduce_model: type["Model"] | None = None
    reproduce_name: str | None = None

    @classmethod
    def get_reproduce_topic(cls):
        """
        creates a simple default reproducer topic (consumer) for this producer

        for more complex setup, subclass `ReproduceTopic` directly
        """

        if cls.reproduce_name:
            _name = cls.reproduce_name
        elif cls.reproduce_model:
            _name = cls.reproduce_model._meta.db_table
        else:
            raise ValueError("Either reproduce_name or reproduce_model must be set")

        if cls.reproduce_namespace:
            _name = f"{cls.reproduce_namespace}.{_name}"

        class _ReproduceTopic(ReproduceTopic):
            name = _name
            reproducer = cls()

        return _ReproduceTopic()

    def reproduce(self, key: Any, value: Any, is_deletion: bool) -> None:
        """receives the consumed message data and reproduces it as needed

        calls `_reproduce_upsert` or `_reproduce_deletion`, which must be implemented
        """

        if is_deletion:
            self._reproduce_deletion(key["id"], key, value)
            return

        # suppress; upsert messages aren't relevant to reproduce if the instance is gone
        with contextlib.suppress(self.reproduce_model.DoesNotExist):
            instance = self.reproduce_model.objects.get(id=key["id"])
            self._reproduce_upsert(instance, key, value)

    def _reproduce_upsert(self, instance, key, value):
        """produce message for instance upsert"""
        raise NotImplementedError

    def _reproduce_deletion(self, instance_id: int, key, value):
        """produce message for instance deletion"""
        raise NotImplementedError
