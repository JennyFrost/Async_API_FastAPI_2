import re
from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis

from models.film import Film, FilmBase
from services.redis_mixins import CacheMixin, Paginator, Sort
from services.elastic_class import ElasticMain

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService(CacheMixin):

    async def get_by_id(self, film_id: str) -> Film | None:
        film = await self._object_from_cache(film_id)
        if not film:
            film = await self.elastic_main.get_obj_from_elastic(film_id, "movies", Film)
            if not film:
                return None
            await self._put_object_to_cache(film, film_id)
        return film
    
    async def get_films_query(self, page: int, page_size: int, query: str) -> list[FilmBase]:
        key_for_cache = f"films_page_{page}_size_{page_size}_query_{query}"
        films = await self._objects_from_cache(key_for_cache)
        if not films:
            films = await self.elastic_main.get_objects_query_from_elastic(
                query=query, query_field='title', page=page, page_size=page_size, some_class=FilmBase, index='movies'
            )
            if not films:
                return []
            await self._put_objects_to_cache(films, key_for_cache)
        return films
    
    async def get_films_page(self, page: int, size: int, sort_field: str, genre: str):
        """
        Метод возвращает запрошенную страницу с фильмами, определенного размера
        """
        key_for_cache = f"films_page_{page}_size_{size}_sort_{sort_field}_genre_{genre}"
        page_films = await self._objects_from_cache(key_for_cache)
        if not page_films:
            dict_filter = None
            if genre:
                dict_filter = {'genre': genre}
            page_films = await self.elastic_main.get_objects_from_elastic(
                page=page, page_size=size,
                index='movies', some_class=FilmBase,
                dict_filter=dict_filter,
                sort_field=sort_field,
                )
            if not page_films:
                return []
            await self._put_objects_to_cache(page_films, key_for_cache)
        return page_films

    async def get_person_films(
            self, person_id: str,
            page_size: int,
            page_number: int,
            sort_field: str) -> list[FilmBase]:
        person_films = await self._objects_from_cache(
            f'person_films_page_{page_number}_size_{page_size}_' + person_id
        )
        if not person_films:
            person_films = await self.elastic_main.get_objects_from_elastic(
                page=page_number, page_size=page_size,
                index='movies', some_class=FilmBase,
                dict_filter={"actors": person_id, "director": person_id, "writers": person_id},
                sort_field=sort_field)
            await self._put_objects_to_cache(
                person_films,
                f'person_films_page_{page_number}_size_{page_size}_' + person_id)
        return person_films

    async def _object_from_cache(self, some_id: str) -> FilmBase | None:
        obj = await super()._object_from_cache(some_id)
        if obj:
            film = FilmBase.parse_raw(obj)
            return film
    
    async def _objects_from_cache(self, some_id: str) -> list[FilmBase]:
        objects = await super()._objects_from_cache(some_id)
        films = [FilmBase.parse_raw(obj) for obj in objects]
        return films


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    
    elastic_main: ElasticMain = ElasticMain(elastic)
    return FilmService(redis, elastic, elastic_main)
