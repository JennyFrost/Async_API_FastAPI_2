import pytest

from elasticsearch import AsyncElasticsearch

from functional.settings import test_settings_movies
import time


# @pytest.fixture(scope='session')
# async def create_index_movies():
#     es_client = AsyncElasticsearch(hosts=test_settings_movies.es_host)
#     if not es_client.indices.exists(index=test_settings_movies.es_index):
#         await es_client.indices.create(index=test_settings_movies.es_index, body=test_settings_movies.es_index_mapping)
#         print('yes')
#     print('no')
#     yield es_client
#     await es_client.close()
#     # time.sleep(20)
#     # await es_client.indices.delete(index=test_settings_movies.es_index)
#     # await es_client.close()


@pytest.fixture(scope='session')
async def create_index_movies(es_client_conn):
    if es_client_conn.indices.exists(index=test_settings_movies.es_index):
        await es_client_conn.indices.create(index=test_settings_movies.es_index, body=test_settings_movies.es_index_mapping)
        print('yes')
    print('no')
    # return es_client_conn


@pytest.fixture
def es_write_data(es_client_conn):
    async def inner(data: list[dict]):
        actions = []
        for doc in data:
            action = {
                "index": {
                    '_index': 'movies',
                    '_id': doc['id']
                }
            }
            action.update(doc)
            actions.append(action)
        if not await es_client_conn.indices.exists(index=test_settings_movies.es_index):
            await es_client_conn.indices.create(index=test_settings_movies.es_index,
                                                body=test_settings_movies.es_index_mapping)
            print('yes')
        print('no')
        # await es_client_conn.delete_by_query(index=test_settings_movies.es_index, body={"query": {"match_all": {}}})
        response = await es_client_conn.bulk(body=actions, refresh=True)
        if response['errors']:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner
