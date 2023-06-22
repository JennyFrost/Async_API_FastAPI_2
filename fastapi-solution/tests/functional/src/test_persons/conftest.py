# import pytest
#
# from elasticsearch import AsyncElasticsearch
#
# from functional.settings import test_settings_movies
# import time
#
#
# @pytest.fixture
# def es_write_data(create_index_movies):
#     async def inner(data: list[dict]):
#         actions = []
#         for doc in data:
#             action = {
#                 "index": {
#                     '_index': 'movies',
#                     '_id': doc['id']
#                 }
#             }
#             action.update(doc)
#             actions.append(action)
#         response = await es_client.bulk(body=actions, refresh=True)
#         if response['errors']:
#             raise Exception('Ошибка записи данных в Elasticsearch')
#     return inner
