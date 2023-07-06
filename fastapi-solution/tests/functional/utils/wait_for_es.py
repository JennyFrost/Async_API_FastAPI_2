
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import time
import os

load_dotenv()

host=os.getenv('ELASTIC_HOST')
port=os.getenv('ELASTIC_PORT')
wait_time=int(os.getenv("WAIT_TIME", 20))

counter = 1
while counter < wait_time:
    es_client = Elasticsearch(hosts=f"http://{host}:{port}")
    if es_client.ping():
        break
    time.sleep(counter)
    counter += 1

if counter >= wait_time:
    print("False")
else:
    print("True")
