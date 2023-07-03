import os
# from pydantic import Field
from pydantic_settings import BaseSettings
from .testdata import es_mapping
from dotenv import load_dotenv

load_dotenv()


class TestSettings(BaseSettings):
    es_host: str = os.getenv('ELASTIC_HOST')
    es_port: str = os.getenv('ELASTIC_PORT')
    es_index: str
    es_id_field: str
    es_index_mapping: dict

    redis_host: str = os.getenv('REDIS_HOST')
    redis_port: str = os.getenv('REDIS_PORT')
    service_url: str = os.getenv('SERVICE_URL')
    service_port: str = os.getenv('SERVICE_URL_PORT')

    def __init__(self, **data):
        super().__init__(**data)
        self.service_url = f'http://{self.service_url}:{self.service_port}'
 

test_settings_person = TestSettings(
    es_index='persons',
    es_id_field='uuid',
    es_index_mapping=es_mapping.persons_index
) 

test_settings_genre = TestSettings(
    es_index='genres',
    es_id_field='uuid',
    es_index_mapping=es_mapping.genre_index
) 

test_settings_movies = TestSettings(
    es_index='movies',
    es_id_field='id',
    es_index_mapping=es_mapping.index_movies
)
