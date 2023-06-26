import pytest
from elasticsearch import AsyncElasticsearch

from .settings import test_settings_movies
from redis import Redis
import aiohttp


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope='session')
async def es_client_conn():
    es_client = AsyncElasticsearch(hosts=f'http://{test_settings_movies.es_host}:{test_settings_movies.es_port}')
    async with es_client as conv:
        yield conv


@pytest.fixture(scope='session')
async def redis_client_conn():
    redis_client = Redis(host=test_settings_movies.redis_host, port=test_settings_movies.redis_port)
    yield redis_client
    redis_client.close()


@pytest.fixture
def reset_redis(redis_client_conn):
    async def inner():
        redis_client_conn.flushall()
    return inner


@pytest.fixture
def http_request():
    async def inner(query_data, url_path):
        session = aiohttp.ClientSession()
        url = test_settings_movies.service_url + url_path
        async with session.get(url, params=query_data) as response:
            body = await response.json()
            headers = response.headers
            status = response.status
        await session.close()
        return {'body': body, 'status': status}
    return inner


@pytest.fixture
def es_write_data(es_client_conn, es_delete_index_film):
    async def inner(data: list[dict], index_name: str, index_mapping: dict):
        await es_delete_index_film(index_name=index_name)
        await es_client_conn.indices.create(index=index_name,
                                            body=index_mapping)
        response = await es_client_conn.bulk(body=data)
        if response['errors']:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner


@pytest.fixture
def es_delete_index_film(es_client_conn):
    async def inner(index_name):
        if await es_client_conn.indices.exists(index=index_name):
            await es_client_conn.indices.delete(index=index_name)
    return inner



