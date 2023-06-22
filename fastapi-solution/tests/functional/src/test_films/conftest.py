import pytest

from elasticsearch import AsyncElasticsearch

from functional.settings import test_settings_movies
import time


@pytest.fixture(scope='session')
async def create_index_movies():
    es_client = AsyncElasticsearch(hosts=test_settings_movies.es_host)
    if not es_client.indices.exists(index=test_settings_movies.es_index):
        await es_client.indices.create(index=test_settings_movies.es_index, body=test_settings_movies.es_index_mapping)
        print('yes')
    print('no')
    return es_client
    # time.sleep(20)
    # await es_client.indices.delete(index=test_settings_movies.es_index)
    # await es_client.close()
