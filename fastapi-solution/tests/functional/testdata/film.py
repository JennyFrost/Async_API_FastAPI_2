from datetime import date

import uuid
from pydantic import BaseModel


class PersonBase(BaseModel):
    id: str
    name: str


class Genre(BaseModel):
    id: str
    name: str


class FilmBase(BaseModel):
    id: str
    title: str
    imdb_rating: float | None


class Film(FilmBase):
    description: str | None
    creation_date: date | None = None
    genre: list[Genre] | None = []
    actors: list[PersonBase] | None = []
    writers: list[PersonBase] | None = []
    director: list[PersonBase] | None = []


class BaseFilmRequest(BaseModel):
    uuid: uuid.UUID
    title: str
    imdb_rating: float | None


class PersonBaseRequest(BaseModel):
    uuid: str
    full_name: str


class GenreRequest(BaseModel):
    uuid: str
    name: str


class FilmRequest(BaseModel):
    uuid: str
    title: str
    imdb_rating: float | None
    description: str | None
    creation_date: date | None = None
    actors: list[PersonBaseRequest] | None = []
    writers: list[PersonBaseRequest] | None = []
    directors: list[PersonBaseRequest] | None = []
    genre: list[GenreRequest] | None = []
