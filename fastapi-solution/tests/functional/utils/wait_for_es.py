
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import time
import os

load_dotenv()

host=os.getenv('ELASTIC_HOST')
port=os.getenv('ELASTIC_PORT')

while True:
    es_client = Elasticsearch(hosts=f"http://{host}:{port}")
    if es_client.ping():
        break
    time.sleep(1)
