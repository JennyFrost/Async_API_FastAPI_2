import datetime
import time
import uuid
import json

import aiohttp
import pytest
import requests
import time

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from ...settings import test_settings_movies


#  Название теста должно начинаться со слова `test_`
#  Любой тест с асинхронными вызовами нужно оборачивать декоратором `pytest.mark.asyncio`, который следит за запуском и работой цикла событий.

@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The star', 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'Mashed'},
                {'status': 200, 'length': 0}
        )
    ]
)
@pytest.mark.anyio
async def test_search(query_data, expected_answer, es_write_data, generate_random_data):
    es_data = await generate_random_data(60)
    await es_write_data(es_data)
    time.sleep(1)
    session = aiohttp.ClientSession()
    url = test_settings_movies.service_url + '/api/v1/films/search'
    async with session.get(url, params=query_data) as response:
        body = await response.json()
        headers = response.headers
        status = response.status
        print(response.real_url)
    await session.close()
    assert status == expected_answer['status']
    assert len(body['result']) == expected_answer['length']
