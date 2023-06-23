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
async def test_search(query_data, expected_answer, es_write_data, generate_random_data):
    # 1. Генерируем данные для ES
    x = await generate_random_data(60)
    await es_write_data(x)
    #
    session = aiohttp.ClientSession()
    url = test_settings_movies.service_url + '/api/v1/films'
    async with session.get(url, params=query_data) as response:
        body = await response.json()
        headers = response.headers
        status = response.status
    await session.close()

    assert status == expected_answer['status']
    assert len(body['result']) == expected_answer['length']
