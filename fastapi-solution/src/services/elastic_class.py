from elasticsearch import AsyncElasticsearch, NotFoundError
from pydantic import BaseModel


class ElasticMain:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_obj_from_elastic(self, obj_id: str, index: str, some_class) -> BaseModel | None:
        try:
            doc = await self.elastic.get(index=index, id=obj_id)
        except NotFoundError:
            return None
        return some_class(**doc['_source'])

    async def get_objects_from_elastic(self, page: int, page_size: int, index: str, some_class, sort_field: str = None) -> list[BaseModel]:
        try:
            search_body = {"_source": [
                    "id",
                    "title",
                    "imdb_rating"],
                }
            search_body.update({"query": {"match_all": {}}})
            search_body.update(Paginator(page_size=page_size, page_number=page).get_paginate_body())
            if sort_field:
                search_body.update(Sort(sort_field=sort_field).get_sort_body())
            objects = await self.elastic.search(index=index, body=search_body)
        except NotFoundError:
            return None
        return [some_class(**obj['_source']) for obj in objects['hits']['hits']]


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