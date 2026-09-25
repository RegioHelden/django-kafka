from functools import cached_property
from typing import TYPE_CHECKING

from confluent_kafka.serialization import MessageField
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Model

from django_kafka.models.model_sync.fields import ExcludeFields, IncludeFields
from django_kafka.models.model_sync.transforms import RelationTransform, Transform
from django_kafka.relations_resolver.relation import ModelRelation
from django_kafka.topic.avro import AvroTopicConsumer
from django_kafka.topic.model import ModelTopicConsumer

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


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
        transforms: list[Transform] | None = None,
    ):
        self.name = name
        self.model = model
        self.model_sync = sync
        self.transforms = transforms or []

    def get_lookup_kwargs(self, model, key, value) -> dict:
        lookup_kwargs = super().get_lookup_kwargs(model, key, value)
        rewrites = {t.source: t.lookup for t in self.relation_transforms if t.lookup}
        return {rewrites.get(field, field): val for field, val in lookup_kwargs.items()}

    def get_relations(self, msg):
        """
        Yield the relation each RelationTransform will wait on.

        A step after a relation may depend on the instance it resolves, so the
        walk applies the chain as it goes, and stops as soon as a lookup finds
        no row - the relations left are yielded on the replay that follows.
        """
        msg_key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        msg_value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        if self.is_deletion(self.model, msg_key, msg_value):
            return

        remaining_relations = len(self.relation_transforms)
        for transform in self.transforms:
            if isinstance(transform, RelationTransform):
                # waiting on a null id would park the message forever
                if (id_value := msg_value.get(transform.source)) is not None:
                    yield ModelRelation(
                        transform.model,
                        id_field=transform.id_field,
                        id_value=id_value,
                    )
                remaining_relations -= 1
                if not remaining_relations:
                    return
            try:
                # empty key, as in `transform`, so both passes see the same input
                msg_value = transform.apply(self.model_sync, {}, msg_value)[1]
            except ObjectDoesNotExist:
                # only a relation lookup means the awaited row is missing
                if not isinstance(transform, RelationTransform):
                    raise
                return

    @cached_property
    def relation_transforms(self) -> list[RelationTransform]:
        return [t for t in self.transforms if isinstance(t, RelationTransform)]

    @property
    def use_relations_resolver(self) -> bool:
        return bool(self.relation_transforms)

    def _is_field_excluded(self, field):
        fields = self.model_sync.fields if self.model_sync else None
        if isinstance(fields, ExcludeFields):
            return field in fields
        return False

    def transform(self, model, value) -> dict:
        # the resolver has let the message through, so the lookups find their rows
        for transform in self.transforms:
            # ModelTopicConsumer hands us no key
            value = transform.apply(self.model_sync, {}, value)[1]
        return self._field_filter(value)

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
