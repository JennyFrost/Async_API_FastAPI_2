import pytest
from elasticsearch import AsyncElasticsearch

from .settings import test_settings_movies


@pytest.fixture(scope='session')
async def es_client_conn():
    es_client = AsyncElasticsearch(hosts=test_settings_movies.es_host)
    yield es_client
    await es_client.close()