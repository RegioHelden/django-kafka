from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import Flag, auto
from functools import cached_property
from typing import TYPE_CHECKING, cast

from django.apps import apps

if TYPE_CHECKING:
    from django.contrib.contenttypes.models import ContentType
    from django.db.models.base import Model


class MessagePart(Flag):
    KEY = auto()
    VALUE = auto()
    BOTH = KEY | VALUE


class LazyContentTypeMapping(Mapping[int, int], ABC):
    data: Mapping

    def __getitem__(self, key: int, /) -> int:
        return self._resolved_data[key]

    def __len__(self) -> int:
        return len(self.data)

    @cached_property
    def _resolved_data(self) -> dict[int, int]:
        return self._resolve()

    @abstractmethod
    def _resolve(self) -> dict[int, int]: ...

    @staticmethod
    def _get_content_type_ids(models: Iterable[type[Model] | str]) -> list[int | None]:
        model_classes = [
            cast("type[Model]", apps.get_model(model))
            if isinstance(model, str)
            else model
            for model in models
        ]
        content_types = cast(
            "type[ContentType]",
            apps.get_model("contenttypes.contenttype"),
        ).objects.get_for_models(*model_classes)
        return [content_types[model].id for model in model_classes]


@dataclass
class LazySourceContentTypeMapping(LazyContentTypeMapping):
    """Lazy auto-resolving mapping from source content type ID to target model class."""

    data: Mapping[type[Model] | str, int]

    def __iter__(self) -> Iterator[int]:
        yield from self._resolved_data

    def _resolve(self) -> dict[int, int]:
        content_type_ids = self._get_content_type_ids(self.data)
        return dict(zip(content_type_ids, self.data.values(), strict=True))


@dataclass
class LazyTargetContentTypeMapping(LazyContentTypeMapping):
    """Lazy auto-resolving mapping from source model class to target content type ID."""

    data: Mapping[int, type[Model] | str]

    def __iter__(self) -> Iterator[int]:
        yield from self.data

    def _resolve(self) -> dict[int, int]:
        content_type_ids = self._get_content_type_ids(self.data.values())
        return dict(zip(self.data, content_type_ids, strict=True))
