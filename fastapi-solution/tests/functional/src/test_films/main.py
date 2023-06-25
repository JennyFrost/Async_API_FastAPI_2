import json
import random
import string
import uuid
from datetime import datetime

from functional.testdata.film import Film, Genre, PersonBase

documents = []
for _ in range(2):
    genres = [Genre(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
              range(3)]
    actors = [PersonBase(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
              range(3)]
    writers = [PersonBase(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
              range(3)]
    director = [PersonBase(id=str(uuid.uuid4()), name=''.join(random.choices(string.ascii_letters, k=5))) for _ in
              range(3)]
    doc = Film(
        id=str(uuid.uuid4()),
        title='The Star',
        imdb_rating=random.uniform(0, 10),
        description=''.join(random.choices(string.ascii_letters, k=20)),
        creation_date=datetime.now().strftime('%Y-%m-%d'),
        genre=genres,
        actors=actors,
        writers=writers,
        director=director,
        actors_names=[i.name for i in actors],
        writers_names=[i.name for i in writers],
    )
    documents.append({'index': {'_index': 'movies', '_id': doc.id}})
    documents.append(doc.dict())

print(documents)