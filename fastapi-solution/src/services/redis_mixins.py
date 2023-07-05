import json
from pydantic import BaseModel
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch
from services.elastic_class import ElasticMain


class MainServiceMixin:

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch, elastic_main: ElasticMain):
        self.redis = redis
        self.elastic = elastic
        self.elastic_main = elastic_main


class CacheMixin(MainServiceMixin):

    async def _object_from_cache(self, some_id: str) -> bytes:
        data = await self.redis.get(some_id)
        if not data:
            return None
        return data
    
    async def _objects_from_cache(self, some_id: str) -> list[str]:
        data = await self.redis.get(some_id)
        if not data:
            return []
        objects = json.loads(data)
        return objects

    async def _put_object_to_cache(self, obj: BaseModel, some_id: str, time_cache: int = 30):
        await self.redis.set(some_id, obj.json(), time_cache)

    async def _put_objects_to_cache(self, objects: list[BaseModel], some_id: str, time_cache: int = 60):
        print('-----------------------------------------------------')
        print(type(objects))
        print(objects)
        objects = [obj.json() for obj in objects]
        objects = json.dumps(objects)
        await self.redis.set(some_id, objects, time_cache)



