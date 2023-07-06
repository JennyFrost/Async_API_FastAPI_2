import re
from abc import ABC, abstractmethod

from elasticsearch import AsyncElasticsearch, NotFoundError, exceptions
from pydantic import BaseModel


class AsyncDBEngine(ABC):
    @abstractmethod
    async def get_by_id(self, obj_id: str, index: str, some_class) -> BaseModel | None:
        pass

    @abstractmethod
    async def get_queryset(self, index, some_class):
        pass

    @abstractmethod
    def filter(self):
        pass

    @abstractmethod
    def paginate(self, page: int, page_size: int):
        pass

    @abstractmethod
    def all(self):
        pass

    @abstractmethod
    def sort(self, sort_field):
        pass

    @abstractmethod
    def search(self, query, query_field):
        pass


class ElasticMain:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic
        self.search_body = {}

    async def get_by_id(self, obj_id: str, index: str, some_class) -> BaseModel | None:
        """достать один элемент по id"""
        try:
            doc = await self.elastic.get(index=index, id=obj_id)
        except NotFoundError:
            return None
        return some_class(**doc['_source'])

    async def get_queryset(self, index, some_class=None):
        if some_class:
            self.search_body.update({"_source": list(some_class.__fields__.keys())})
        try:
            objects = await self.elastic.search(index=index, body=self.search_body)
        except exceptions.NotFoundError:
            objects = {'hits': {'hits': []}}
        if some_class is None:
            return objects
        return [some_class(**obj['_source']) for obj in objects['hits']['hits']]

    def get_all(self):
        self.search_body = ({"query": {"match_all": {}}})
        return self

    def paginate(self, page: int, page_size: int):
        if not self.search_body:
            self.get_all()
        self.search_body.update(Paginator(page_size=page_size, page_number=page).get_paginate_body())
        return self

    def filter(self, dict_filter):
        if dict_filter:
            self.get_objects_filter_from_elastic(dict_filter=dict_filter)
        return self

    def sort(self, sort_field):
        if not self.search_body:
            self.get_all()
        self.search_body.update(Sort(sort_field=sort_field).get_sort_body())
        return self

    def search(self, query, query_field):
        query = re.sub(' +', '~ ', query.rstrip()).rstrip() + '~'
        self.search_body = {}
        self.search_body.update({"query": {
            "query_string": {
                "default_field": query_field,
                "query": query
            }
        }})
        return self

    def inner_objects(self, source_field: str):
        [i['nested'].update({"inner_hits": {}}) for i in self.search_body['query']['bool']['should']]
        self.search_body.update({"_source": [source_field]})

    def get_objects_filter_from_elastic(self, dict_filter: dict) -> None:
        should = []
        for filter_field, filter_by in dict_filter.items():
            should.append({
                "nested": {
                    "path": filter_field,
                    "query": {
                        "term": {
                            f"{filter_field}.id": filter_by
                        }
                    }
                }
            })
        self.search_body['query'] = {
            "bool": {
                "should": should
            }
        }


class Paginator(BaseModel):
    page_number: int
    page_size: int

    def get_paginate_body(self) -> dict:
        start_number = (self.page_number - 1) * self.page_size
        start_number = start_number if start_number > 0 else 0
        paginator_body = {
            "from": start_number,
            "size": self.page_size
        }
        return paginator_body


class Sort(BaseModel):
    sort_field: str
    order: str = "asc"

    def get_sort_body(self) -> dict:
        if self.sort_field[0] == '-':
            self.order = "desc"
            self.sort_field = self.sort_field[1:]
        sort_body = {
            "sort": [
                    {
                        self.sort_field: {
                            "order": self.order
                        }
                    }
                    ]
        }
        return sort_body
