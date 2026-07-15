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
    Declarative FK relation for a PythonSink.

    Standard FK relations are auto-detected from the model; only provide
    explicit Relation entries to override or customise auto-detection:
      - non-default `id_field` (e.g. lookup by `kafka_uuid` instead of `pk`)
      - renamed message field (`value_field`) after enrich transforms
      - nullable/blank FK fields (excluded from auto-detection by default)

    `fk` is the merge key: it identifies which FK field on the model this
    entry is for, replaces the auto-detected entry for that field, and emits
    a `RelationTransform` that swaps the raw message value for the resolved
    model instance assigned to `fk`.

    `lookup`: when set, `get_lookup_kwargs` rewrites `value_field` → `lookup`
    in the ORM lookup kwargs. Use when the message key name doesn't match the
    model lookup path (e.g. `user__kafka_uuid` → `customer_user__kafka_uuid`).

    Null (or absent) message values yield no relation - a null FK has
    nothing to resolve - and the emitted `RelationTransform` assigns
    `None` to `fk`.
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

    def to_model_relation(self, msg_value: dict) -> ModelRelation | None:
        """
        Return the ModelRelation the resolver should wait for, or None when
        the message value is null or absent - a null FK has nothing to
        resolve, and a relation with id_value=None can never exist, so
        waiting on it would park the message forever.
        """
        id_value = msg_value.get(self.value_field)
        if id_value is None:
            return None
        return ModelRelation(
            self.model,
            id_field=self.id_field,
            id_value=id_value,
        )


class PythonSinkTopicBase(ModelTopicConsumer):
    """
    Base class for ModelSync-generated PythonSink topics.

    `sync`, `relations`, `transforms` arrive via `__init__` from
    `PythonSink.make_topic`. `relations` is the fully resolved list
    (auto-detected FK relations merged with any explicit overrides).
    They drive:
        - `get_relations` (resolver waits on FK prerequisites),
        - `get_lookup_kwargs` (remap key names per `Relation.lookup`),
        - `transform` (run consume pipeline, then drop fields not allowed
          by `sync.fields` or produced by any transform).
    """

    # Override the abstract `name` property on TopicConsumer so the
    # combined class is concrete; `__init__` then sets the real value.
    name: str | None = None
    deletion_key = "__deleted"

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
        self.model_sync = sync
        self.relations = relations or []
        self.transforms = transforms or []

    def get_lookup_kwargs(self, model, key, value) -> dict:
        rewrites = {r.value_field: r.lookup for r in self.relations if r.lookup}
        return {rewrites.get(field, field): val for field, val in key.items()}

    def get_relations(self, msg):
        msg_key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        msg_value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        if self.is_deletion(self.model, msg_key, msg_value):
            return
        for relation in self.relations:
            if model_relation := relation.to_model_relation(msg_value):
                yield model_relation

    @property
    def use_relations_resolver(self) -> bool:
        return bool(self.relations)

    def transform(self, model, msg_value) -> dict:
        # ModelTopicConsumer hands us only the value (key was used for the
        # lookup). Pass an empty dict for msg_key so consume_<field> methods
        # accepting both args don't break.
        for transform_step in self.transforms:
            msg_value = transform_step.apply(self.model_sync, {}, msg_value)[1]
        return self._field_filter(msg_value)

    @cached_property
    def _field_filter(self):
        # Allow-list driven by the sync's `fields` plus everything any
        # transform (enricher, consume, FK) writes. Stale fields not
        # declared and not produced are dropped before they reach the
        # model so old topic messages can't overwrite live columns.
        fields = self.model_sync.fields if self.model_sync else None
        if fields is None:
            return lambda message: message
        produces: set[str] = set()
        for transform_step in self.model_sync.enrich_transforms:
            produces |= transform_step.produces(self.model_sync)
        for transform_step in self.transforms:
            produces |= transform_step.produces(self.model_sync)
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
    """Avro-deserializing topic over the ModelSync-driven base."""
