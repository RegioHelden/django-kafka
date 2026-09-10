from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Transform

if TYPE_CHECKING:
    from django_kafka.models.model_sync import ModelSync


class TopicTransformsMixin:
    """
    Resolves and applies a list of Transforms against a ModelSync.
    """

    def __init__(
        self,
        *args,
        transforms: list[Transform] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.transforms: list[Transform] = transforms or []

    def apply_transforms(
        self,
        sync: ModelSync,
        msg_key: dict | None,
        msg_value: dict | None,
    ) -> tuple[dict | None, dict | None]:
        """Apply transforms in declared order, returning the final pair."""
        for transform in self.transforms:
            msg_key, msg_value = transform.apply(sync, msg_key or {}, msg_value or {})
        return msg_key, msg_value

    def update_schema(
        self,
        sync: ModelSync,
        key_fields: list[dict],
        value_fields: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Walk transforms to derive the post-transform Avro schemas."""
        for transform in self.transforms:
            key_fields, value_fields = transform.update_schema(
                sync,
                key_fields,
                value_fields,
            )
        return key_fields, value_fields
