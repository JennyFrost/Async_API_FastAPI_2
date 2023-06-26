import pytest
import uuid
import random
import string
from datetime import datetime

from ...testdata.film import Film, Genre, PersonBase
from ...settings import test_settings_movies


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
            documents.append({'index': {'_index': test_settings_movies.es_index, '_id': doc.id}})
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
