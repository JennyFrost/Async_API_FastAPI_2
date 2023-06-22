import datetime
import time
import uuid
import json

import aiohttp
import pytest

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from ...settings import test_settings_movies


#  Название теста должно начинаться со слова `test_`
#  Любой тест с асинхронными вызовами нужно оборачивать декоратором `pytest.mark.asyncio`, который следит за запуском и работой цикла событий.

@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The Star', 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'Mashed potato'},
                {'status': 200, 'length': 0}
        )
    ]
)
@pytest.mark.asyncio
async def test_search(query_data, expected_answer, es_write_data):
    # 1. Генерируем данные для ES

    es_data = [{
        'id': str(uuid.uuid4()),
        'imdb_rating': 8.5,
        'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}],
        'title': 'The Star',
        'description': 'New World',
        'director': [
            {'id': '111', 'name': 'Ann'},
            {'id': '222', 'name': 'Bob'}
        ],
        'actors_names': ['Ann', 'Bob'],
        'writers_names': ['Ben', 'Howard'],
        'actors': [
            {'id': '111', 'name': 'Ann'},
            {'id': '222', 'name': 'Bob'}
        ],
        'writers': [
            {'id': '333', 'name': 'Ben'},
            {'id': '444', 'name': 'Howard'}
        ],
        'creation_date': datetime.datetime.now().isoformat(),
        # 'updated_at': datetime.datetime.now().isoformat(),
        # 'film_work_type': 'movie'
    } for _ in range(100)]

    await es_write_data(es_data)

    session = aiohttp.ClientSession()
    url = test_settings_movies.service_url + '/api/v1/films/search'
    async with session.get(url, params=query_data) as response:
        body = await response.json()
        headers = response.headers
        status = response.status
    await session.close()

    assert status == expected_answer['status']
    assert len(body['result']) == expected_answer['length']
