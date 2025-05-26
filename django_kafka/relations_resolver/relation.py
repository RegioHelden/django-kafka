from pydoc import locate

from django.db.models import Model

from django_kafka.conf import settings
from django_kafka.relations_resolver.cache.base import RelationResolverCache


def get_relation_resolver_cache() -> RelationResolverCache | None:
    if settings.RELATION_RESOLVER_CACHE:
        return locate(settings.RELATION_RESOLVER_CACHE)()
    return None


class ModelRelation:
    model: type[Model]
    object_id: int
    id_field: str
    model_path: str
    identifier: str

    def __init__(self, model: type[Model], object_id: int | str, id_field: str):
        self.model = model
        self.object_id = object_id
        self.id_field = id_field
        self.model_path = f"{model.__module__}.{self.model.__name__}"
        self.identifier = f"{self.model_path}-{self.object_id}"
        self.cache = get_relation_resolver_cache()

    async def get(self) -> Model:
        return await self.model.objects.aget(**{self.id_field: self.object_id})

    async def exists(self) -> bool:
        return await self.model.objects.filter(
            **{self.id_field: self.object_id},
        ).aexists()

    async def has_waiting_messages(self) -> bool:
        return await self.cache.exists(self.identifier)

    async def mark_as_awaited(self):
        await self.cache.add(self.identifier)

    async def cleanup(self):
        await self.cache.delete(self.identifier)
