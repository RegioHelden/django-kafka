from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_kafka.models.model_sync.sync import ModelSync


class DescriptorWrapper:
    """
    Binds Source/Sink instances to their owning ModelSync on attribute access.

    Source/Sink methods (`db_table`, `topics`, etc.) often need to read sync
    config (model, topic, fields). On instance access, returns a clone with
    `instance` set; on class access, returns the unbound declaration. Stored
    `_kwargs` lets the clone faithfully replay the original constructor args.
    """

    instance: "ModelSync | None"

    def __init__(self, instance: "ModelSync | None" = None, **kwargs):
        self.instance = instance
        self._kwargs = kwargs

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return self.__class__(instance=obj, **self._kwargs)
