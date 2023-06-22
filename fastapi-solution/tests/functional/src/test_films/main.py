from elasticsearch import AsyncElasticsearch, helpers
import asyncio
async def index_documents():
    # Создаем подключение к Elasticsearch
    es = AsyncElasticsearch(hosts='http://127.0.0.1:9200')
    # Определяем набор документов для индексации
    documents = [
        {
            '_index': 'movies',
            '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5, 'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}], 'title': 'The Star', 'description': 'New World', 'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'], 'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}], 'creation_date': '2023-06-22T23:50:26.029418'}
        },
        {
            '_index': 'movies',
            '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5, 'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}], 'title': 'The Star', 'description': 'New World', 'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'], 'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}], 'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}], 'creation_date': '2023-06-22T23:50:26.029418'}
        },
        {
            '_index': 'movies',
            '_source': {'id': 'bc46bd7e-8ab5-41a8-8932-990d67ad5930', 'imdb_rating': 8.5,
                        'genre': [{'id': '112', 'name': 'Action'}, {'id': '113', 'name': 'Sci-Fi'}],
                        'title': 'The Star', 'description': 'New World',
                        'director': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}],
                        'actors_names': ['Ann', 'Bob'], 'writers_names': ['Ben', 'Howard'],
                        'actors': [{'id': '111', 'name': 'Ann'}, {'id': '222', 'name': 'Bob'}],
                        'writers': [{'id': '333', 'name': 'Ben'}, {'id': '444', 'name': 'Howard'}],
                        'creation_date': '2023-06-22T23:50:26.029418'}
        }
    ]
    # Используем метод bulk() для индексации документов
    x = await helpers.async_bulk(es, documents)
    # Закрываем соединение с Elasticsearch
    await es.close()

async def main():
    await index_documents()
asyncio.run(main())