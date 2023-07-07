from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.person import Role, Person, PersonFilm
from services.redis_mixins import CacheMixin
from services.elastic_class import ElasticMain, RedisMain

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 2


class PersonService(CacheMixin):

    async def search_person(
            self, search_text: str,
            page: int, page_size: int) -> list[Person]:
        persons = await self.redis_conn.get_by_id(obj_id='search_person_' + search_text, some_class=Person, many=True)
        if not persons:
            # persons = await self.db.get_objects_query_from_elastic(
            #     query=search_text, query_field='full_name', page=page, page_size=page_size, some_class=Person, index='persons'
            # )
            self.db.search(query=search_text, query_field='full_name').paginate(page=page, page_size=page_size)
            persons = await self.db.get_queryset(index='persons', some_class=Person)
            result = []
            for person in persons:
                person = await self._get_person_roles_from_elastic(person)
                result.append(person)
            persons = result
            if not persons:
                return []
            await self.redis_conn.create(obj=persons, some_id='search_person_' + search_text, many=True)
        return persons

    async def get_by_id(self, person_id: str) -> Person | None:
        person = await self.redis_conn.get_by_id(obj_id=person_id, some_class=Person)
        if not person:
            person = await self.db.get_by_id(person_id, "persons", Person)
            if not person:
                return None
            person = await self._get_person_roles_from_elastic(person)
            await self.redis_conn.create(obj=person, some_id=person_id)
        return person

    async def _get_person_roles_from_elastic(self, person: Person) -> Person | None:
        # films = await self.db.inner_objects_elastic(
        #     source_field='id',
        #     dict_filter={"actors": person.uuid, "director": person.uuid, "writers": person.uuid},
        #     index='movies')
        self.db.filter({"actors": person.uuid, "director": person.uuid, "writers": person.uuid}).inner_objects(source_field='id')
        films = await self.db.get_queryset(index='movies')

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

    async def _object_from_cache(self, some_id: str) -> Person | None:
        obj = await super()._object_from_cache(some_id)
        if obj:
            person = Person.parse_raw(obj)
            return person

    async def _objects_from_cache(self, some_id: str) -> list[Person]:
        objects = await super()._objects_from_cache(some_id)
        persons = [Person.parse_raw(obj) for obj in objects]
        return persons


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    
    db: ElasticMain = ElasticMain(elastic)
    redis_conn: Redis = RedisMain(redis)
    return PersonService(redis, elastic, db, redis_conn)
