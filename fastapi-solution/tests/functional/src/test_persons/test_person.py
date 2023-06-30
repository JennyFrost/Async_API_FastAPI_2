import pytest
import asyncio
from ...settings import test_settings_movies, test_settings_person


@pytest.mark.parametrize(
    'data_for_write, query_data, expected_answer, reset_redis_flag',
    [
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Tom', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Tom"},
                False
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {"query": "Sara"},
                {"status": 200, "person": "Sara"},
                False
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'slArk', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Clark"},
                True
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Jhon', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Jhon"},
                False
        ),
    ]
)
@pytest.mark.anyio
async def test_search_persons(data_for_write, query_data, expected_answer, reset_redis_flag, es_write_data, http_request, es_delete_index_film, gen_data_person_tests, reset_redis): 
    # 0) создаем данные для загрузки
    data: dict = await gen_data_person_tests(data_for_write)
    # assert len(query_data) > 0, data_for_write
    # 1) создаем индекс фильмов и загружаем туда данные
    await es_write_data(data=data.get("movies"), index_name=test_settings_movies.es_index, index_mapping=test_settings_movies.es_index_mapping)
    # 2) создаем индекс персон и загружаем туда данные
    await es_write_data(data=data.get("persons"), index_name=test_settings_person.es_index, index_mapping=test_settings_person.es_index_mapping)
    # 3) делаем запрос к API сервиса
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/persons/search')
    # 4) удаляем индексы
    if reset_redis_flag:
        await reset_redis()
    else:
        await es_delete_index_film(index_name=test_settings_movies.es_index)
        await es_delete_index_film(index_name=test_settings_person.es_index)
    # 5) проверяем статус сообщения
    assert response['status'] == expected_answer['status']
    # 6) проверяем корректность данных
    assert response['body'][0]['full_name'] == expected_answer["person"]
