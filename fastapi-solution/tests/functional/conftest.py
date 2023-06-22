# import pytest
#
# from elasticsearch import AsyncElasticsearch
#
# from functional.settings import test_settings_movies
#
#
# @pytest.fixture
# async def create_index_movies():
#     print('''asdas''')
#     return 'sad'
#     # es_client = AsyncElasticsearch(hosts=test_settings_movies.es_host)
#     # await es_client.indices.create(index=test_settings_movies.es_index, body=test_settings_movies.es_index_mapping)