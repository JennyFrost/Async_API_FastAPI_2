import re
from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.person import Role, Person, PersonFilm
from services.redis_mixins import CacheMixin, Paginator
from services.elastic_class import ElasticMain

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 2


class PersonService(CacheMixin):

    async def search_person(
            self, search_text: str,
            page: int, page_size: int) -> list[Person]:
        persons = await self._objects_from_cache('search_person_' + search_text)
        if not persons:
            text = re.sub(' +', '~ ', search_text.rstrip()).rstrip() + '~'
            persons = await self._search_person_from_elastic(text, page, page_size)
            if not persons:
                return []
            await self._put_objects_to_cache(persons, 'search_person_' + search_text)
        return persons

    async def get_by_id(self, person_id: str) -> Person | None:
        person = await self._object_from_cache(person_id)
        if not person:
            person = await self.elastic_main.get_obj_from_elastic(person_id, "persons", Person)
            person = await self._get_person_roles_from_elastic(person)
            if not person:
                return None
            await self._put_object_to_cache(person, person_id)
        return person

    async def _get_person_from_elastic(self, person_id: str) -> Person | None:
        try:
            doc = await self.elastic.get(index='persons', id=person_id)
            person = Person(**doc['_source'])
            person = await self._get_person_roles_from_elastic(person)
        except NotFoundError:
            return None
        return person

    async def _search_person_from_elastic(
            self, search_text: str,
            page: int, page_size: int) -> list[Person]:
        search_body = {
                "query": {
                    "query_string": {
                        "default_field": "full_name",
                        "query": search_text
                    }
                }
            }
        search_body.update(Paginator(page_size=page_size, page_number=page).get_paginate_body())
        persons = await self.elastic.search(
            index='persons',
            body=search_body
        )
        result = []
        for person in persons['hits']['hits']:
            person = Person(**person['_source'])
            person = await self._get_person_roles_from_elastic(person)
            result.append(person)
        return result

    async def _get_person_roles_from_elastic(self, person: Person) -> Person | None:
        films = await self.elastic.search(
            index='movies',
            body={
                "_source": ["id"],
                "query": {
                    "bool": {
                        "should": [
                            {
                                "nested": {
                                    "path": "actors",
                                    "query": {
                                        "term": {
                                            "actors.id": person.uuid
                                        }
                                    },
                                    "inner_hits": {}
                                }
                            },
                            {
                                "nested": {
                                    "path": "director",
                                    "query": {
                                        "term": {
                                            "director.id": person.uuid
                                        }
                                    },
                                    "inner_hits": {}
                                }
                            },
                            {
                                "nested": {
                                    "path": "writers",
                                    "query": {
                                        "term": {
                                            "writers.id": person.uuid
                                        }
                                    },
                                    "inner_hits": {}
                                }
                            }
                        ]
                    }
                }
            }
        )
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
    
    elastic_main: ElasticMain = ElasticMain(elastic)

    return PersonService(redis, elastic, elastic_main)
