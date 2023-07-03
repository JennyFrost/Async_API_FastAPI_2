from pydantic import BaseSettings, Field
from .testdata import es_mapping
from dotenv import load_dotenv

load_dotenv()


class TestSettings(BaseSettings):
    es_host: str = Field(default='http://127.0.0.1', env='ELASTIC_HOST')
    es_port: str = Field(default='9200', env='ELASTIC_PORT')
    es_index: str
    es_id_field: str
    es_index_mapping: dict

    redis_host: str = Field(default='', env='REDIS_HOST')
    redis_port: str = Field(default='', env='REDIS_PORT')
    service_url: str = Field(default='', env='SERVICE_URL')
    service_port: str = Field(default='', env='SERVICE_URL_PORT')

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
