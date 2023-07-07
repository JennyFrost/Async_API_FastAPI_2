from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre
from services.connector_db import ElasticMain, RedisMain

GENRES_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService:
    def __init__(self, search_engine, cache):
        self.search_engine = search_engine
        self.cache = cache

    async def get_by_id(self, genre_id: str) -> Genre | None:
        genre = await self.cache.get_by_id(obj_id=genre_id, some_class=Genre)
        if not genre:
            genre = await self.search_engine.get_by_id(obj_id=genre_id, index="genres", some_class=Genre)
            if not genre:
                return None
            await self.cache.create(obj=genre, some_id=genre_id)

        return genre

    async def get_genres_list(self, page: int, page_size: int) -> list[Genre]:
        genres = await self.cache.get_by_id(obj_id=f'all_genres_page_{page}_size_{page_size}', some_class=Genre, many=True)
        if not genres:
            self.search_engine.get_all().paginate(page=page, page_size=page_size)
            genres = await self.search_engine.get_queryset(index='genres', some_class=Genre)
            if not genres:
                return []
            await self.cache.create(obj=genres, some_id=f'all_genres_page_{page}_size_{page_size}', many=True)
        return genres


@lru_cache()
def get_genres_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    
    search_engine: ElasticMain = ElasticMain(elastic)
    cache: Redis = RedisMain(redis)
    return GenreService(search_engine, cache)
