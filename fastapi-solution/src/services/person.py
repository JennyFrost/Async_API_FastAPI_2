from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.person import Role, Person, PersonFilm
from services.connector_db import ElasticMain, RedisMain

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 2


class PersonService:
    def __init__(self, search_engine, cache):
        self.search_engine = search_engine
        self.cache = cache

    async def search_person(
            self, search_text: str,
            page: int, page_size: int) -> list[Person]:
        persons = await self.cache.get_by_id(obj_id='search_person_' + search_text, some_class=Person, many=True)
        if not persons:
            self.search_engine.search(query=search_text, query_field='full_name').paginate(page=page, page_size=page_size)
            persons = await self.search_engine.get_queryset(index='persons', some_class=Person)
            result = []
            for person in persons:
                person = await self._get_person_roles_from_elastic(person)
                result.append(person)
            persons = result
            if not persons:
                return []
            await self.cache.create(obj=persons, some_id='search_person_' + search_text, many=True)
        return persons

    async def get_by_id(self, person_id: str) -> Person | None:
        person = await self.cache.get_by_id(obj_id=person_id, some_class=Person)
        if not person:
            person = await self.search_engine.get_by_id(person_id, "persons", Person)
            if not person:
                return None
            person = await self._get_person_roles_from_elastic(person)
            await self.cache.create(obj=person, some_id=person_id)
        return person

    async def _get_person_roles_from_elastic(self, person: Person) -> Person | None:
        self.search_engine.filter(
            {"actors": person.uuid, "director": person.uuid, "writers": person.uuid}).inner_objects(source_field='id')
        films = await self.search_engine.get_queryset(index='movies')

        for film in films['hits']['hits']:
            film_id = film['_source']['id']
            person_film = PersonFilm(uuid=film_id)
            if film['inner_hits']['actors']['hits']['total']['value'] == 1:
                person_film.roles.append(Role.actor)
            if film['inner_hits']['writers']['hits']['total']['value'] == 1:
                person_film.roles.append(Role.writer)
            if film['inner_hits']['director']['hits']['total']['value'] == 1:
                person_film.roles.append(Role.director)
            person.films.append(person_film)
        return person


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    
    search_engine: ElasticMain = ElasticMain(elastic)
    cache: Redis = RedisMain(redis)
    return PersonService(search_engine, cache)
