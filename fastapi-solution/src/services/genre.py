from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre
from services.elastic_class import ElasticMain

from services.redis_mixins import CacheMixin, Paginator

GENRES_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService(CacheMixin):

    async def get_by_id(self, genre_id: str) -> Genre | None:
        genre = await self._object_from_cache(genre_id)
        if not genre:
            genre = await self.elastic_main.get_obj_from_elastic(genre_id, "genres", Genre)
            if not genre:
                return None
            await self._put_object_to_cache(genre, genre_id)

        return genre

    async def get_genres_list(self, page: int, page_size: int) -> list[Genre]:
        genres = await self._objects_from_cache(f'all_genres_page_{page}_size_{page_size}')
        if not genres:
            genres = await self._get_genres_from_elastic(page, page_size)
            if not genres:
                return []
            await self._put_objects_to_cache(genres, f'all_genres_page_{page}_size_{page_size}')
        return genres

    async def _get_genre_from_elastic(self, genre_id: str) -> Genre | None:
        try:
            doc = await self.elastic.get(index='genres', id=genre_id)
        except NotFoundError:
            return None
        return Genre(**doc['_source'])

    async def _get_genres_from_elastic(self, page: int, page_size: int) -> list[Genre]:
        try:
            search_body = {"query": {"match_all": {}}}
            search_body.update(Paginator(page_size=page_size, page_number=page).get_paginate_body())
            genres = await self.elastic.search(index='genres', body=search_body)
        except NotFoundError:
            return None
        return [Genre(**genre['_source']) for genre in genres['hits']['hits']]
    
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
    
    elastic_main: ElasticMain = ElasticMain(elastic)

    return GenreService(redis, elastic, elastic_main)
