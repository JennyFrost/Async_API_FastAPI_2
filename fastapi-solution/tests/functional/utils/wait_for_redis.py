from redis import Redis
from dotenv import load_dotenv
import time
import os

load_dotenv()

host=os.getenv('REDIS_HOST')
port=os.getenv('REDIS_PORT')
wait_time=os.getenv("WAIT_TIME", 20)

counter = 1
while counter < wait_time:
    redis_conn = Redis(host=host, port=port)
    if redis_conn.ping():
        break
    time.sleep(counter)
    counter += 1

if counter == wait_time:
    print("False")
else:
    print("True")