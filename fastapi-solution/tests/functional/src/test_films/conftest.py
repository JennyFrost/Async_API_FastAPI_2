import pytest
import uuid
import random
import string
import aiohttp
from datetime import datetime

from elasticsearch import AsyncElasticsearch
from redis import Redis

from ...settings import test_settings_movies
from ...testdata.film import Film, Genre, PersonBase


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
    async def inner(data: list[dict]):
        await es_delete_index_film()
        await es_client_conn.indices.create(index=test_settings_movies.es_index,
                                            body=test_settings_movies.es_index_mapping)
        response = await es_client_conn.bulk(body=data)
        if response['errors']:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner


@pytest.fixture
def es_delete_index_film(es_client_conn):
    async def inner():
        if await es_client_conn.indices.exists(index=test_settings_movies.es_index):
            await es_client_conn.indices.delete(index=test_settings_movies.es_index)
    return inner


@pytest.fixture
def generate_films_filter_genre(generate_films, generate_genre):
    async def inner(num_documents, genre_id):
        genres = await generate_genre(2)
        genres.append(Genre(id=genre_id, name=''.join(random.choices(string.ascii_letters, k=20))))
        return await generate_films(num_documents=num_documents, genres=genres)
    return inner


@pytest.fixture
def generate_films(generate_person, generate_genre):
    async def inner(num_documents,
                    title: str = None,
                    actors: list[PersonBase] = None,
                    writers: list[PersonBase] = None,
                    director: list[PersonBase] = None,
                    genres: list[Genre] = None):
        documents = []
        for _ in range(num_documents):
            if actors is None:
                actors = await generate_person(3)
            if writers is None:
                writers = await generate_person(3)
            if director:
                director = await generate_person(3)
            if genres is None:
                genres = await generate_genre(3)
            doc = Film(
                id=str(uuid.uuid4()),
                title=''.join(random.choices(string.ascii_letters, k=7)) if title is None else title,
                imdb_rating=random.uniform(1, 10),
                description=''.join(random.choices(string.ascii_letters, k=20)),
                creation_date=datetime.now().strftime('%Y-%m-%d'),
                genre=genres,
                actors=actors,
                writers=writers,
                director=director
            )
            documents.append({'index': {'_index': 'movies', '_id': doc.id}})
            documents.append(doc.dict())
        return documents
    return inner


@pytest.fixture(scope='session')
def generate_person():
    async def inner(count):
        return [PersonBase(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
         range(count)]
    return inner


@pytest.fixture(scope='session')
def generate_genre():
    async def inner(count) -> list[Genre]:
        return [Genre(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
         range(count)]
    return inner
