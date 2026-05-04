from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

from confluent_kafka.serialization import MessageField
from django.db.models import Model

from django_kafka.models.model_sync.fields import ExcludeFields, IncludeFields
from django_kafka.models.model_sync.transforms import RelationTransform, Transform
from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.topic.avro import AvroTopicConsumer
from django_kafka.topic.model import ModelTopicConsumer

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


@dataclass
class Relation:
    """
    Declarative foreign-key relation for a PythonSink.

    When `fk` is set: emits a `RelationTransform` into the consume pipeline
    that replaces the message field with the related instance, assigned to
    `fk`. Use this when the message field name doesn't match a column on
    the consuming model (e.g. `user__kafka_uuid` in message -> `user=<User>`
    on the model).

    When `lookup` is set: framework's default `get_lookup_kwargs` rewrites
    `value_field` -> `lookup` in the lookup kwargs. Use when the message
    key field name doesn't match the model lookup path (e.g.
    `user__kafka_uuid` in message -> `customer_user__kafka_uuid` lookup).
    """

    model: type[Model]
    id_field: str
    value_field: str
    fk: str | None = None
    lookup: str | None = None

    def to_transform(self) -> "RelationTransform | None":
        """Return the Transform that performs FK lookup, if `fk` is set."""
        if not self.fk:
            return None
        return RelationTransform(
            source=self.value_field,
            target=self.fk,
            model=self.model,
            id_field=self.id_field,
        )


class PythonSinkTopicBase(ModelTopicConsumer):
    """
    Base class for ModelSync-generated PythonSink topics.

    `sync`, `relations`, `transforms` arrive via `__init__` from
    `PythonSink.make_topic`. They drive:
        - `get_relations` (resolver waits on FK prerequisites),
        - `get_lookup_kwargs` (remap key names per `Relation.lookup`),
        - `transform` (run consume pipeline, then drop fields not allowed
          by `sync.fields` or produced by any transform).
    """

    # Override the abstract `name` property on TopicConsumer so the
    # combined class is concrete; `__init__` then sets the real value.
    name: str | None = None

    def __init__(
        self,
        *,
        name: str,
        model: type[Model],
        sync: "ModelSync",
        relations: list[Relation] | None = None,
        transforms: list[Transform] | None = None,
    ):
        self.name = name
        self.model = model
        self.sync = sync
        self.relations = relations or []
        self.transforms = transforms or []

    def is_deletion(self, model, key, value) -> bool:
        if value is None:
            return True
        deleted = value.pop("__deleted", None)
        if isinstance(deleted, bool):
            return deleted
        if isinstance(deleted, str):
            return deleted.lower() == "true"
        return False

    def get_lookup_kwargs(self, model, key, value) -> dict:
        rewrites = {
            r.value_field: r.lookup
            for r in (self.relations or ()) if r.lookup
        }
        return {rewrites.get(field, field): val for field, val in key.items()}

    def get_relations(self, msg):
        msg_key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        msg_value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        if self.is_deletion(self.model, msg_key, msg_value):
            return
        for relation in self.relations or ():
            yield ModelRelation(
                relation.model,
                id_field=relation.id_field,
                id_value=msg_value[relation.value_field],
            )

    @property
    def use_relations_resolver(self) -> bool:
        return bool(self.relations)

    def transform(self, model, msg_value) -> dict:
        # ModelTopicConsumer hands us only the value (key was used for the
        # lookup). Pass an empty dict for msg_key so consume_<field> methods
        # accepting both args don't break.
        for transform_step in self.transforms or ():
            msg_value = transform_step.apply(self.sync, {}, msg_value, "consume")[1]
        return self._field_filter(msg_value)

    @cached_property
    def _field_filter(self):
        # Allow-list driven by the sync's `fields` plus everything any
        # transform (enricher, consume, FK) writes. Stale fields not
        # declared and not produced are dropped before they reach the
        # model so old topic messages can't overwrite live columns.
        fields = self.sync.fields if self.sync else None
        if fields is None:
            return lambda message: message
        produces: set[str] = set()
        for transform_step in self.sync.enrich_transforms:
            produces |= transform_step.produces(self.sync, "enrich")
        for transform_step in self.transforms or ():
            produces |= transform_step.produces(self.sync, "consume")
        if isinstance(fields, IncludeFields):
            allowed = set(fields) | produces
            return lambda message: {
                name: value for name, value in message.items() if name in allowed
            }
        if isinstance(fields, ExcludeFields):
            excluded = set(fields) - produces
            return lambda message: {
                name: value for name, value in message.items() if name not in excluded
            }
        return lambda message: message


class PythonSinkAvroTopicConsumer(AvroTopicConsumer, PythonSinkTopicBase):
    """
    Default topic class for `PythonAvroSink` — Avro deserialization over the ModelSync-driven base.
    """
