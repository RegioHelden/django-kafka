import redis

from django_kafka.conf import settings
from django_kafka.relations_resolver.cache.base import RelationResolverCache

redis_client = redis.Redis(settings.REDIS_CONF)


class RedisRelationCache(RelationResolverCache):
    async def add(self, relation_identifier: str):
        redis_client.set(relation_identifier, 1)

    async def delete(self, relation_identifier):
        redis_client.delete(relation_identifier)

    async def exists(self, relation_identifier: str) -> bool:
        return redis_client.get(relation_identifier)
