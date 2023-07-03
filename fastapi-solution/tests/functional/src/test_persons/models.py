from pydantic import BaseModel
import uuid


class ResponsePerson(BaseModel):
    uuid: uuid.UUID
    full_name: str
    films: list


class ResponsePersonFilm(BaseModel):
    uuid: uuid.UUID
    title: str
    imdb_rating: float | None