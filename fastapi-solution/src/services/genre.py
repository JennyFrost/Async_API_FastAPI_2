from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre
from services.elastic_class import ElasticMain, RedisMain

GENRES_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService:
    def __init__(self, db, redis_conn):
        self.db = db
        self.redis_conn = redis_conn

    async def get_by_id(self, genre_id: str) -> Genre | None:
        genre = await self.redis_conn.get_by_id(obj_id=genre_id, some_class=Genre)
        if not genre:
            genre = await self.db.get_by_id(obj_id=genre_id, index="genres", some_class=Genre)
            if not genre:
                return None
            await self.redis_conn.create(obj=genre, some_id=genre_id)

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
            await self.redis_conn.create(obj=genres, some_id=f'all_genres_page_{page}_size_{page_size}', many=True)
        return genres


@lru_cache()
def get_genres_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    
    db: ElasticMain = ElasticMain(elastic)
    redis_conn: Redis = RedisMain(redis)
    return GenreService(db, redis_conn)
