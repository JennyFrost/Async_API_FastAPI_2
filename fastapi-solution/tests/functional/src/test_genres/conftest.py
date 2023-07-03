import pytest

from ...testdata.film import GenreRequest
from ...settings import test_settings_genre, test_settings_movies

genres = ["Western",
          "Adventure",
          "Drama",
          "Romance",
          "Sport",
          "Talk-Show",
          "Action",
          "Thriller",
          "Comedy",
          "Family",
          "Music",
          "Crime",
          "Animation",
          "Sci-Fi",
          "Documentary",
          "Musical",
          "Short",
          "Fantasy",
          "War",
          "Biography",
          "Mystery",
          "Reality-TV",
          "History",
          "News",
          "Horror",
          "Game-Show"]


@pytest.fixture
def generate_genres():
    async def inner(num_documents,
                    genre_id: str = None,
                    name: str = None):
        if num_documents > len(genres):
            num_documents = len(genres)
        documents_genres = []
        for i in range(num_documents):
            doc = GenreRequest(
                uuid=str(i)*3 if genre_id is None else genre_id,
                name=genres[i] if name is None else name
            )
            documents_genres.append({'index': {'_index': test_settings_genre.es_index,
                                               '_id': doc.uuid}})
            documents_genres.append(doc.dict())
        return documents_genres
    return inner
