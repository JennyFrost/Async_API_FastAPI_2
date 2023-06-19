import os
from pydantic import BaseSettings, Field
from logging import config as logging_config
from dotenv import load_dotenv

load_dotenv()

from .logger import LOGGING

logging_config.dictConfig(LOGGING)


class Settings(BaseSettings):
    redis_host: str = Field(..., env='REDIS_HOST')
    redis_port: int = Field(..., env='REDIS_PORT')
    elastic_host: str = Field(..., env='ELASTIC_HOST')
    elastic_port: int = Field(..., env='ELASTIC_PORT')
    project_name: str = Field(..., env='PROJECT_NAME')
    base_dir: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    page_size: int = 20
    sort_field: str = "-imdb_rating"

    class Config:
        env_file = '.env.debug'


settings = Settings()

