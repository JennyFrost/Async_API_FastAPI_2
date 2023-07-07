from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis

from models.film import Film, FilmBase
from services.connector_db import ElasticMain, RedisMain

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService:
    def __init__(self, elastic_conn, redis_conn):
        self.elastic_conn = elastic_conn
        self.redis_conn = redis_conn

    async def get_by_id(self, film_id: str) -> Film | None:
        film = await self.redis_conn.get_by_id(obj_id=film_id, some_class=FilmBase)
        if not film:
            film = await self.elastic_conn.get_by_id(film_id, "movies", Film)
            if not film:
                return None
            await self.redis_conn.create(obj=film, some_id=film_id)
        return film
    
    async def get_films_query(self, page: int, page_size: int, query: str) -> list[FilmBase]:
        key_for_cache = f"films_page_{page}_size_{page_size}_query_{query}"
        films = await self.redis_conn.get_by_id(obj_id=key_for_cache, some_class=FilmBase, many=True)
        if not films:
            self.elastic_conn.search(query=query, query_field='title').paginate(page=page, page_size=page_size)
            films = await self.elastic_conn.get_queryset(index='movies', some_class=FilmBase)
            if not films:
                return []
            await self.redis_conn.create(obj=films, some_id=key_for_cache, many=True)
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
            self.elastic_conn.get_all().paginate(page=page, page_size=size).filter(dict_filter=dict_filter).sort(sort_field=sort_field)
            page_films = await self.elastic_conn.get_queryset(index='movies', some_class=FilmBase)
            if not page_films:
                return []
            await self.redis_conn.create(obj=page_films, some_id=key_for_cache, many=True)
        return page_films

    async def get_person_films(
            self, person_id: str,
            page_size: int,
            page_number: int,
            sort_field: str) -> list[FilmBase]:
        person_films = await self.redis_conn.get_by_id(
            obj_id=f'person_films_page_{page_number}_size_{page_size}_' + person_id, some_class=FilmBase, many=True)
        if not person_films:
            self.elastic_conn.get_all().paginate(page=page_number, page_size=page_size).filter(
                dict_filter={"actors": person_id, "director": person_id, "writers": person_id}).sort(
                sort_field=sort_field)
            person_films = await self.elastic_conn.get_queryset(index='movies', some_class=FilmBase)
            await self.redis_conn.create(
                obj=person_films,
                some_id=f'person_films_page_{page_number}_size_{page_size}_' + person_id, many=True)
        return person_films


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    
    elastic_conn: ElasticMain = ElasticMain(elastic)
    redis_conn: Redis = RedisMain(redis)
    return FilmService(elastic_conn, redis_conn)
