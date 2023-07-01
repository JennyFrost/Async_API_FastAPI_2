import pytest
import uuid
import random
import string
from datetime import datetime, date
from pydantic import BaseModel
from ...testdata.film import Genre, PersonBase
from ...settings import test_settings_movies, test_settings_person
from dataclasses import dataclass
from enum import Enum


class TestsDataPersons(Enum):
    NAMES_PERSONS = ["Nikole", "Adam", "Tom", "Samuil", "Robert", "Kris", "Sofia"]
    TITLE_MOVIES = ["Titanic", "Star Wars", "Gladiator", "Seven"]
    NAME_GENRES = ["Drama", "Sifi", "Fantasy", "Action", "Documental", "Scary"]


class FilmBase(BaseModel):
    id: str
    title: str
    imdb_rating: float | None


class PersonForIndex(BaseModel):
    uuid: str
    full_name: str


class Film(FilmBase):
    description: str | None
    creation_date: date | None = None
    genre: list[Genre] | None = []
    actors: list[PersonBase] | None = []
    writers: list[PersonBase] | None = []
    director: list[PersonBase] | None = []


@dataclass
class TestData:
    movies_title: list[str]
    actors: list[str]
    writers: list[str]
    directors: list[str]
    genres: list[str]


async def convertation_persons(persons: list[PersonForIndex]) -> list[PersonBase]:
    return [PersonBase(id=pers.uuid, name=pers.full_name) for pers in persons]
   

async def generate_genre(count) -> list[Genre]:
    NAME_GENRES = ["Drama", "Sifi", "Fantasy", "Action", "Documental", "Scary"]
    if count > len(NAME_GENRES):
        count = len(NAME_GENRES)
    return [Genre(id=str(uuid.uuid4()), name=name) for name in
        random.choices(NAME_GENRES, k=count)]


async def generate_random_person(count) -> list[Genre]:
    return [PersonForIndex(uuid=str(uuid.uuid4()), full_name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
        range(count)]


async def gen_doc_films(titles_movies: list[str], persons: list[PersonForIndex]) -> list[dict]:
    documents = []
    persons = await convertation_persons(persons)
    for movie in titles_movies:
        film = Film(
            id=str(uuid.uuid4()),
            title=movie,
            description=''.join(random.choices(string.ascii_letters, k=20)),
            creation_date=datetime.now().strftime('%Y-%m-%d'),
            genre=await generate_genre(3),
            actors=persons,
            writers=persons,
            director=persons,
        )
        documents.append({'index': {'_index': test_settings_movies.es_index, '_id': film.id}})
        documents.append(film.dict())
    return documents


async def gen_doc_persons(persons: list[PersonForIndex]):
    documents = []
    for pers in persons:
        documents.append({'index': {'_index': test_settings_person.es_index, '_id': pers.uuid}})
        documents.append(pers.dict())
    return documents
    # return inner


# 1) Нужно сделать универсальный метод, который принимает структуру {names: ["name_person1", "name_person2"...], movies: ["film1", "film2"...]}
# 2) Метод возвращает словарь {persons_data: [данные для персон], movies_data: [данные для фильмов]}
@pytest.fixture
def gen_data_person_tests():
    async def inner(data: dict):
        prime_person = [PersonForIndex(uuid=str(uuid.uuid4()), full_name=name_person) for name_person in data.get("names")]
        persons_: dict = {model_p.full_name: model_p.uuid for model_p in prime_person}
        movies_person_doc = await gen_doc_films(data.get("movies"), prime_person)
        prime_person_doc = await gen_doc_persons(prime_person)
        return {"movies": movies_person_doc, "persons": prime_person_doc, "persons_": persons_}
    return inner




