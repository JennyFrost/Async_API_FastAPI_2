from functools import wraps
import time
import os
import json
from typing import Any
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from log_pack import log_error, log_success

class DslEl(BaseModel):
    host: str
    port: int

class StoragePlace(BaseModel):
    dump_path: str
    index_path: str

def load_env():
    '''Загружает переменные окружения'''

    load_dotenv()

    el_dsl = DslEl(
        host=os.getenv('ELASTIC_HOST'),
        port=os.getenv('ELASTIC_PORT')
    )

    storage_places = StoragePlace(
        dump_path=os.getenv('DUMP'),
        index_path=os.getenv('INDEX')
    )
    return el_dsl, storage_places


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции

    """
    # Декоратор работает с методами PostgresManage.get_conn_pg и ElasticMain.elastic_client
    # Они возвращают либо экземпляр клиента или коннектор либо None
    # Если они будут возвращать None то цикл будет работать бесконечно
    def func_wrapper(func):
        @wraps(func)
        def inner(*arg):
            t = start_sleep_time
            argument = arg[1] if len(arg) > 1 else arg[0]
            while True:
                conn = func(argument)
                if conn:
                    return conn
                time.sleep(t)
                if t < border_sleep_time:
                    t = t * factor
                else:
                    t = border_sleep_time
        return inner
    return func_wrapper 


class JsonFileStorage:
    """Реализация хранилища, использующего локальный файл.
    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, "w") as json_file:
            json_file.write(json.dumps(state))

    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища. Возвращает сырой словарь"""
        try:
            with open(self.file_path) as json_file:
                data = json_file.read()
                if not data:
                    raise Exception
                data_dict = json.loads(data)
                return data_dict
        except Exception as err:
            log_error(err)
            return dict()