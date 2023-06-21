from redis import Redis
from dotenv import load_dotenv
import time
import os

load_dotenv()

host=os.getenv('REDIS_HOST')
port=os.getenv('REDIS_PORT')

while True:
    redis_conn = Redis(host=host, port=port)
    if redis_conn.ping():
        break
    time.sleep(1)