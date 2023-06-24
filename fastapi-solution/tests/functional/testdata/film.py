from datetime import date

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
