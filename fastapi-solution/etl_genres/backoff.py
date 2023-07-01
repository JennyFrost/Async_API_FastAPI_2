from datetime import datetime
from functools import wraps
import elasticsearch
import psycopg2
import time

import state
from etl_logging import logger


def sleep_func(time_to_sleep: int):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            while True:
                storage = state.JsonFileStorage(file_path='current_state.json')
                cur_state = state.State(storage)
                func(*args, **kwargs)
                end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur_state.set_state('finished_at', end)
                time.sleep(time_to_sleep)

        return inner
    return func_wrapper


def backoff_break(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    def func_wrapper(func):
        @wraps(func)
        def inner(self, *args, time_to_start: str, was_error: bool = False, recs: list[str] = []):
            t = start_sleep_time
            storage = state.JsonFileStorage(file_path='current_state.json')
            cur_state = state.State(storage)
            while True:
                if t < border_sleep_time:
                    t *= factor
                else:
                    t = border_sleep_time
                value = func(self, *args, time_to_start=time_to_start, was_error=was_error, recs=recs)
                if 'error' in value:
                    time.sleep(t)
                    recs = value[1]
                    time_to_start = value[2]
                    cur_state.set_state('last_date', time_to_start)
                    was_error = True
                else:
                    return value

        return inner
    return func_wrapper


def backoff(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                if t < border_sleep_time:
                    t = t * factor
                else:
                    t = border_sleep_time
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, elasticsearch.exceptions.ConnectionError,
                        elasticsearch.exceptions.ConnectionTimeout):
                    logger.error('Database does not respond! Trying again')
                    time.sleep(t)
        return inner
    return func_wrapper
