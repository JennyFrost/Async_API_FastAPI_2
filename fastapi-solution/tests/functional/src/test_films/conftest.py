import pytest
import pytest_asyncio
import uuid
import random
import string
from datetime import datetime

from elasticsearch import AsyncElasticsearch

from ...settings import test_settings_movies
import time


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope='session')
async def es_client_conn():
    es_client = AsyncElasticsearch(hosts=f'{test_settings_movies.es_host}:{test_settings_movies.es_port}')
    async with es_client as conv:
        yield conv
    # yield es_client
    # await es_client.close()


@pytest.fixture
def es_write_data(es_client_conn):
    async def inner(data: list[dict]):
        if await es_client_conn.indices.exists(index=test_settings_movies.es_index):
            await es_client_conn.indices.delete(index=test_settings_movies.es_index)
        await es_client_conn.indices.create(index=test_settings_movies.es_index,
                                            body=test_settings_movies.es_index_mapping)
        response = await es_client_conn.bulk(body=data)
        if response['errors']:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner


@pytest.fixture
def generate_random_data():
    async def inner(num_documents):
        documents = []
        for _ in range(num_documents):
            doc = {
                'id': str(uuid.uuid4()),
                'imdb_rating': random.uniform(0, 10),
                'genre': {
                    'id': str(uuid.uuid4()),
                    'name': ''.join(random.choices(string.ascii_letters, k=5))
                },
                'title': 'The Star',
                'description': ''.join(random.choices(string.ascii_letters, k=20)),
                'creation_date': datetime.now().strftime('%Y-%m-%d'),
                'director': {
                    'id': str(uuid.uuid4()),
                    'name': ''.join(random.choices(string.ascii_letters, k=5))
                },
                'actors_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(3)],
                'writers_names': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(2)],
                'actors': [
                    {'id': str(uuid.uuid4()),
                     'name': ''.join(random.choices(string.ascii_letters, k=5))}
                    for _ in range(3)
                ],
                'writers': [
                    {'id': str(uuid.uuid4()),
                     'name': ''.join(random.choices(string.ascii_letters, k=5))}
                    for _ in range(2)
                ]
            }

            documents.append({'index': {'_index': 'movies', '_id': doc['id']}})
            documents.append(doc)
        print(len(documents))
        return documents
    return inner
