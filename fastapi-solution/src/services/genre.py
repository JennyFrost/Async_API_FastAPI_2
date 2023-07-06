from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre
from services.elastic_class import ElasticMain, RedisMain

from services.redis_mixins import CacheMixin

GENRES_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService(CacheMixin):

    async def get_by_id(self, genre_id: str) -> Genre | None:
        genre = await self.redis_conn.get_by_id(obj_id=genre_id, some_class=Genre)
        if not genre:
            genre = await self.db.get_by_id(obj_id=genre_id, index="genres", some_class=Genre)
            if not genre:
                return None
            await self._put_object_to_cache(genre, genre_id)

        return genre

    async def get_genres_list(self, page: int, page_size: int) -> list[Genre]:
        genres = await self.redis_conn.get_by_id(obj_id=f'all_genres_page_{page}_size_{page_size}', some_class=Genre, many=True)
        if not genres:
            # genres = await self.db.get_objects_from_elastic(
            #     page=page, page_size=page_size,
            #     index='genres', some_class=Genre)
            self.db.get_all().paginate(page=page, page_size=page_size)
            genres = await self.db.get_queryset(index='genres', some_class=Genre)
            if not genres:
                return []
            await self._put_objects_to_cache(genres, f'all_genres_page_{page}_size_{page_size}')
        return genres
    
    async def _objects_from_cache(self, some_id: str) -> list[Genre]:
        objects = await super()._objects_from_cache(some_id)
        genres = [Genre.parse_raw(obj) for obj in objects]
        return genres
    
    async def _object_from_cache(self, some_id: str) -> Genre | None:
        obj = await super()._object_from_cache(some_id)
        if obj:
            genre = Genre.parse_raw(obj)
            return genre


@lru_cache()
def get_genres_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    
    db: ElasticMain = ElasticMain(elastic)
    redis_conn: Redis = RedisMain(redis)
    return GenreService(redis, elastic, db, redis_conn)
