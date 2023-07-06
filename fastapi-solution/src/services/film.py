from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis

from models.film import Film, FilmBase
from services.redis_mixins import CacheMixin
from services.elastic_class import ElasticMain, RedisMain

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService(CacheMixin):

    async def get_by_id(self, film_id: str) -> Film | None:
        film = await self.redis_conn.get_by_id(obj_id=film_id, some_class=FilmBase)
        # film = await self._object_from_cache(film_id)
        if not film:
            film = await self.db.get_by_id(film_id, "movies", Film)
            if not film:
                return None
            await self._put_object_to_cache(film, film_id)
        return film
    
    async def get_films_query(self, page: int, page_size: int, query: str) -> list[FilmBase]:
        key_for_cache = f"films_page_{page}_size_{page_size}_query_{query}"
        films = await self.redis_conn.get_by_id(obj_id=key_for_cache, some_class=FilmBase, many=True)
        if not films:
            self.db.search(query=query, query_field='title').paginate(page=page, page_size=page_size)
            films = await self.db.get_queryset(index='movies', some_class=FilmBase)
            if not films:
                return []
            await self._put_objects_to_cache(films, key_for_cache)
        return films
    
    async def get_films_page(self, page: int, size: int, sort_field: str, genre: str):
        """
        Метод возвращает запрошенную страницу с фильмами, определенного размера
        """
        key_for_cache = f"films_page_{page}_size_{size}_sort_{sort_field}_genre_{genre}"
        page_films = await self.redis_conn.get_by_id(obj_id=key_for_cache, some_class=FilmBase, many=True)
        if not page_films:
            dict_filter = None
            if genre:
                dict_filter = {'genre': genre}
            self.db.get_all().paginate(page=page, page_size=size).filter(dict_filter=dict_filter).sort(sort_field=sort_field)
            page_films = await self.db.get_queryset(index='movies', some_class=FilmBase)
            if not page_films:
                return []
            await self._put_objects_to_cache(page_films, key_for_cache)
        return page_films

    async def get_person_films(
            self, person_id: str,
            page_size: int,
            page_number: int,
            sort_field: str) -> list[FilmBase]:
        person_films = await self.redis_conn.get_by_id(obj_id=f'person_films_page_{page_number}_size_{page_size}_' + person_id, some_class=FilmBase, many=True)
        if not person_films:
            self.db.get_all().paginate(page=page_number, page_size=page_size).filter(dict_filter={"actors": person_id, "director": person_id, "writers": person_id}).sort(
                sort_field=sort_field)
            person_films = await self.db.get_queryset(index='movies', some_class=FilmBase)
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
    
    db: ElasticMain = ElasticMain(elastic)
    redis_conn: Redis = RedisMain(redis)
    return FilmService(redis, elastic, db, redis_conn)
