# from elasticsearch import AsyncElasticsearch, helpers
import asyncio
# async def index_documents():
#     # Создаем подключение к Elasticsearch
#     es = AsyncElasticsearch(hosts='http://127.0.0.1:9200')
#     # Определяем набор документов для индексации
#     documents = [
#         {
#             '_index': 'movies',
#             '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5, 'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}], 'title': 'The Star', 'description': 'New World', 'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'], 'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}], 'creation_date': '2023-06-22T23:50:26.029418'}
#         },
#         {
#             '_index': 'movies',
#             '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5, 'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}], 'title': 'The Star', 'description': 'New World', 'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'], 'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}], 'creation_date': '2023-06-22T23:50:26.029418'}
#         },
#         {
#             '_index': 'movies',
#             '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5,
#                         'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}],
#                         'title': 'The Star', 'description': 'New World',
#                         'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}],
#                         'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'],
#                         'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}],
#                         'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}],
#                         'creation_date': '2023-06-22T23:50:26.029418'}
#         }
#     ]
#     # Используем метод bulk() для индексации документов
#     x = await helpers.async_bulk(es, documents)
#     # Закрываем соединение с Elasticsearch
#     await es.close()
#
# async def main():
#     await index_documents()
# asyncio.run(main())


import random
import string
from datetime import datetime
from elasticsearch import AsyncElasticsearch

async def generate_random_data(num_documents):
    documents = []

    for _ in range(num_documents):
        doc = {
            'id': ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
            'imdb_rating': random.uniform(0, 10),
            'genre': {
                'id': ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
                'name': ''.join(random.choices(string.ascii_letters, k=5))
            },
            'title': ''.join(random.choices(string.ascii_letters, k=10)),
            'description': ''.join(random.choices(string.ascii_letters, k=20)),
            'creation_date': datetime.now().strftime('%Y-%m-%d'),
            'director': {
                'id': ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
                'name': ''.join(random.choices(string.ascii_letters, k=5))
            },
            'actors_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(3)],
            'writers_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(2)],
            'actors': [
                {'id': ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
                 'name': ''.join(random.choices(string.ascii_letters, k=5))}
                for _ in range(3)
            ],
            'writers': [
                {'id': ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
                 'name': ''.join(random.choices(string.ascii_letters, k=5))}
                for _ in range(2)
            ]
        }

        documents.append({'index': {'_index': 'movies'}})
        documents.append(doc)

    print(len(documents))
    return documents

async def index_documents(documents):
    # Создание экземпляра клиента AsyncElasticsearch
    es = AsyncElasticsearch(hosts='http://127.0.0.1:9200')
    print(len(documents))
    if await es.indices.exists(index='movies'):
        await es.indices.delete(index='movies')
    await es.indices.create(index='movies',
                                        body={
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "english_possessive_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "imdb_rating": {
        "type": "float"
      },
      "genre": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      },
      "title": {
        "type": "text",
        "analyzer": "ru_en",
        "fields": {
          "raw": {
            "type":  "keyword"
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "creation_date": {
        "type": "date",
        "format": "yyyy-MM-dd"
      },
      "director": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      },
      "actors_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "writers_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "actors": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      },
      "writers": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      }
    }
  }
})

    # Индексация документов одним запросом с использованием метода bulk()
    await es.bulk(body=documents)

    # Закрытие клиента
    await es.close()

# Генерация данных
num_documents = 10
documents = asyncio.run(generate_random_data(num_documents))

# Индексация документов
asyncio.run(index_documents(documents))
