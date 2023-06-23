import asyncio
# import uuid
#
#
#
# import random
# import string
# from datetime import datetime
# from elasticsearch import AsyncElasticsearch
#
# async def generate_random_data(num_documents):
#     documents = []
#
#     for _ in range(num_documents):
#         doc = {
#             'id': str(uuid.uuid4()),
#             'imdb_rating': random.uniform(0, 10),
#             'genre': {
#                 'id': str(uuid.uuid4()),
#                 'name': ''.join(random.choices(string.ascii_letters, k=5))
#             },
#             'title': ''.join(random.choices(string.ascii_letters, k=10)),
#             'description': ''.join(random.choices(string.ascii_letters, k=20)),
#             'creation_date': datetime.now().strftime('%Y-%m-%d'),
#             'director': {
#                 'id': str(uuid.uuid4()),
#                 'name': ''.join(random.choices(string.ascii_letters, k=5))
#             },
#             'actors_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(3)],
#             'writers_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(2)],
#             'actors': [
#                 {'id': str(uuid.uuid4()),
#                  'name': ''.join(random.choices(string.ascii_letters, k=5))}
#                 for _ in range(3)
#             ],
#             'writers': [
#                 {'id': str(uuid.uuid4()),
#                  'name': ''.join(random.choices(string.ascii_letters, k=5))}
#                 for _ in range(2)
#             ]
#         }
#
#         documents.append({'index': {'_index': 'movies', '_id': doc['id']}})
#         documents.append(doc)
#
#     return documents
#
# async def index_documents(documents):
#     # Создание экземпляра клиента AsyncElasticsearch
#     es = AsyncElasticsearch(hosts='http://78.153.130.84:9200')
#     if await es.indices.exists(index='movies'):
#         await es.indices.delete(index='movies')
#     await es.indices.create(index='movies',
#                                         body={
#   "settings": {
#     "refresh_interval": "1s",
#     "analysis": {
#       "filter": {
#         "english_stop": {
#           "type":       "stop",
#           "stopwords":  "_english_"
#         },
#         "english_stemmer": {
#           "type": "stemmer",
#           "language": "english"
#         },
#         "english_possessive_stemmer": {
#           "type": "stemmer",
#           "language": "possessive_english"
#         },
#         "russian_stop": {
#           "type":       "stop",
#           "stopwords":  "_russian_"
#         },
#         "russian_stemmer": {
#           "type": "stemmer",
#           "language": "russian"
#         }
#       },
#       "analyzer": {
#         "ru_en": {
#           "tokenizer": "standard",
#           "filter": [
#             "lowercase",
#             "english_stop",
#             "english_stemmer",
#             "english_possessive_stemmer",
#             "russian_stop",
#             "russian_stemmer"
#           ]
#         }
#       }
#     }
#   },
#   "mappings": {
#     "dynamic": "strict",
#     "properties": {
#       "id": {
#         "type": "keyword"
#       },
#       "imdb_rating": {
#         "type": "float"
#       },
#       "genre": {
#         "type": "nested",
#         "dynamic": "strict",
#         "properties": {
#           "id": {
#             "type": "keyword"
#           },
#           "name": {
#             "type": "text",
#             "analyzer": "ru_en"
#           }
#         }
#       },
#       "title": {
#         "type": "text",
#         "analyzer": "ru_en",
#         "fields": {
#           "raw": {
#             "type":  "keyword"
#           }
#         }
#       },
#       "description": {
#         "type": "text",
#         "analyzer": "ru_en"
#       },
#       "creation_date": {
#         "type": "date",
#         "format": "yyyy-MM-dd"
#       },
#       "director": {
#         "type": "nested",
#         "dynamic": "strict",
#         "properties": {
#           "id": {
#             "type": "keyword"
#           },
#           "name": {
#             "type": "text",
#             "analyzer": "ru_en"
#           }
#         }
#       },
#       "actors_names": {
#         "type": "text",
#         "analyzer": "ru_en"
#       },
#       "writers_names": {
#         "type": "text",
#         "analyzer": "ru_en"
#       },
#       "actors": {
#         "type": "nested",
#         "dynamic": "strict",
#         "properties": {
#           "id": {
#             "type": "keyword"
#           },
#           "name": {
#             "type": "text",
#             "analyzer": "ru_en"
#           }
#         }
#       },
#       "writers": {
#         "type": "nested",
#         "dynamic": "strict",
#         "properties": {
#           "id": {
#             "type": "keyword"
#           },
#           "name": {
#             "type": "text",
#             "analyzer": "ru_en"
#           }
#         }
#       }
#     }
#   }
# })
#
#     # Индексация документов одним запросом с использованием метода bulk()
#     await es.bulk(body=documents)
#
#     # Закрытие клиента
#     await es.close()
#
# # Генерация данных
# num_documents = 10
# documents = asyncio.run(generate_random_data(num_documents))
#
# # Индексация документов
# asyncio.run(index_documents(documents))
import aiohttp

async def first_request():
    session = aiohttp.ClientSession()
    url = 'http://127.0.0.1:8000' + '/api/v1/films'
    async with session.get(url, params={'query': 'The Star', 'page_size': 50}) as response:
        body = await response.json()
        headers = response.headers
        status = response.status
    print(body)
    await session.close()

asyncio.run(first_request())