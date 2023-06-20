from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor

from etl_logging import logger
from backoff import sleep_func, backoff
import state
from settings import PostgresSettings, ElasticSettings, es_settings, index_name

from data_validator import Genre
from postgres_producer import PostgresProducer
from es_loader import ESLoader


class ETLProcess:

    def __init__(self, time_to_sleep: int, dsl: dict, es_path: list,
                 es_settings: dict,
                 index_name: str = index_name):
        self.time_to_sleep = time_to_sleep
        self.dsl = dsl
        self.es_path = es_path
        self.es_settings = es_settings
        self.index_name = index_name

    @backoff()
    def extract(self, time_to_start: str) -> list | None:
        with psycopg2.connect(**self.dsl, cursor_factory=DictCursor) as pg_conn:
            producer = PostgresProducer(time_to_start, pg_conn)
            recs = producer.extract_recs(time_to_start=time_to_start, recs=[])
            if not recs:
                logger.info(f'Nothing changed! Check again in {self.time_to_sleep} seconds')
                return None
        logger.info(f'{len(recs)} records extracted from postgres')
        return recs

    def transform(self, recs) -> list[dict[str: str|dict[str: str|float|dict[str: str]]]]:
        to_es = []
        for rec in recs:
            row_dict = dict(rec)
            genre = Genre(**row_dict)
            to_es.extend(genre.to_es_format(self.index_name))
        return to_es

    @backoff()
    def load(self, data: list[dict[str: str | list[dict[str: str]]]], is_first_time: bool) \
            -> dict[str]:
        loader = ESLoader(self.es_path, self.es_settings, self.index_name)
        if is_first_time:
            loader.create_index()
        return loader.load_data(data)

    @sleep_func(time_to_sleep=10)
    def etl(self) -> dict[str] | None:
        storage = state.JsonFileStorage(file_path='current_state.json')
        cur_state_dict = storage.retrieve_state()
        cur_state = state.State(storage)
        is_first_time = False
        if not cur_state_dict:
            is_first_time = True
            time_to_start = datetime.now() - timedelta(days=5000)
            time_to_start = time_to_start.strftime("%Y-%m-%d %H:%M:%S")
        else:
            if 'last_date' in cur_state_dict:
                time_to_start = cur_state.get_state('last_date')
            else:
                time_to_start = cur_state.get_state('finished_at')
        genres = self.extract(time_to_start=time_to_start)
        if not genres:
            return
        to_es = self.transform(genres)
        return self.load(to_es, is_first_time)


if __name__ == '__main__':
    dsl = PostgresSettings().dict()
    es_path = [ElasticSettings().dict()]
    etl_process = ETLProcess(time_to_sleep=20, dsl=dsl, es_path=es_path, es_settings=es_settings)
    etl_process.etl()