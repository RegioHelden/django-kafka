from django_kafka.models import RelationResolverCacheModel
from django_kafka.relations_resolver.cache.base import RelationResolverCache


class RelationResolverModelCache(RelationResolverCache):
    async def add(self, relation_identifier: str):
        await RelationResolverCacheModel.objects.aget_or_create(
            relation_identifier=relation_identifier,
        )

    async def delete(self, relation_identifier):
        await RelationResolverCacheModel.objects.filter(
            relation_identifier=relation_identifier,
        ).adelete()

    async def exists(self, relation_identifier: str):
        return await RelationResolverCacheModel.objects.filter(
            relation_identifier=relation_identifier,
        ).aexists()
