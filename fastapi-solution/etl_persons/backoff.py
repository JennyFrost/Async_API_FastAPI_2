from time import sleep

import psycopg2
from elasticsearch.exceptions import ConnectionError

from pg_work import PostgresLoad
from es_work import ESWriter
import settings


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def inner_decorator(func):
        def wrapper(*args, **kwargs):
            t = start_sleep_time * 2
            try:
                func(*args, **kwargs)
                return True
            except psycopg2.OperationalError:
                while True:
                    t = t * 2 if t < border_sleep_time else border_sleep_time
                    try:
                        with PostgresLoad(settings.dsl) as pg_conn:
                            break
                    except psycopg2.OperationalError:
                        sleep(t)
            except ConnectionError:
                while True:
                    t = t * 2 if t < border_sleep_time else border_sleep_time
                    try:
                        es = ESWriter(settings.es)
                        es.get_info()
                        break
                    except ConnectionError:
                        sleep(t)
        return wrapper
    return inner_decorator
