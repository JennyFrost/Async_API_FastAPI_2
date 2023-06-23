import pytest
from elasticsearch import AsyncElasticsearch

from .settings import test_settings_movies


# @pytest.fixture(scope='session')
# async def es_client_conn():
#     es_client = AsyncElasticsearch(hosts='http://78.153.130.84:9200')#hosts=f'{test_settings_movies.es_host}:{test_settings_movies.es_port}')
#     yield es_client
#     await es_client.close()