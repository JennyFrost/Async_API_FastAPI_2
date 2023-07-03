import pytest
import asyncio
from ...settings import test_settings_movies, test_settings_person


@pytest.mark.parametrize(
    'data_for_write, query_data, expected_answer',
    [
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Tom', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Tom"},
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {"query": "Sara"},
                {"status": 200, "person": "Sara"},
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'slArk', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Clark"},
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Jhon', 'page_number': 1, 'page_size': 20},
                {"status": 200, "person": "Jhon"},
        ),
    ]
)
@pytest.mark.anyio
async def test_search_persons(data_for_write, query_data, expected_answer, es_write_data, http_request, es_delete_index_film, gen_data_person_tests, reset_redis): 
    # 0) создаем данные для загрузки
    data: dict = await gen_data_person_tests(data_for_write)
    # 1) создаем индекс фильмов и загружаем туда данные
    await es_write_data(data=data.get("movies"), index_name=test_settings_movies.es_index, index_mapping=test_settings_movies.es_index_mapping)
    # 2) создаем индекс персон и загружаем туда данные
    await es_write_data(data=data.get("persons"), index_name=test_settings_person.es_index, index_mapping=test_settings_person.es_index_mapping)
    # 3) делаем запрос к API сервиса
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/persons/search')
    await es_delete_index_film(index_name=test_settings_movies.es_index)
    await es_delete_index_film(index_name=test_settings_person.es_index)

    assert response['status'] == expected_answer['status']
    assert response['body'][0]['full_name'] == expected_answer["person"]

    response = await http_request(query_data, '/api/v1/persons/search')
    assert response['status'] == expected_answer['status']
    assert response['body'][0]['full_name'] == expected_answer["person"]

    await reset_redis()



@pytest.mark.parametrize(
    'data_for_write, query_data, expected_answer',
    [
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Tom'},
                {"status": 200, "person": "Tom"},
        ),
        (
                {"names": ["Robert", "Jhon", "Petro", "Genri"], "movies": ["Iron man", "Tor", "Star Wars"]},
                {'query': 'Genri'},
                {"status": 200, "person": "Genri"},
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Clark'},
                {"status": 200, "person": "Clark"},
        ),
    ]
)
@pytest.mark.anyio
async def test_person_id(data_for_write, query_data, expected_answer, es_write_data, http_request, es_delete_index_film, gen_data_person_tests, reset_redis): 
    data: dict = await gen_data_person_tests(data_for_write)
    await es_write_data(data=data.get("movies"), index_name=test_settings_movies.es_index, index_mapping=test_settings_movies.es_index_mapping)
    await es_write_data(data=data.get("persons"), index_name=test_settings_person.es_index, index_mapping=test_settings_person.es_index_mapping)
    uuid_person = data['persons_'][query_data['query']]
    modify_query = {'query': uuid_person}
    await asyncio.sleep(1)
    response = await http_request(modify_query, '/api/v1/persons/{}'.format(uuid_person))
    assert response['status'] == expected_answer['status']
    assert response['body']['uuid'] == uuid_person
    assert response['body']['full_name'] == query_data['query']
    await es_delete_index_film(index_name=test_settings_movies.es_index)
    await es_delete_index_film(index_name=test_settings_person.es_index)

    response = await http_request(modify_query, '/api/v1/persons/{}'.format(uuid_person))
    assert response['status'] == expected_answer['status']
    assert response['body']['uuid'] == uuid_person
    assert response['body']['full_name'] == query_data['query']

    await reset_redis()        


@pytest.mark.parametrize(
    'data_for_write, query_data, expected_answer',
    [
        (
                {"names": ["Tom", "Jhon", "Sara", "Clark"], "movies": ["Titanic", "Gladiator", "Star Wars"]},
                {'query': 'Tom'},
                {"status": 200, "person": "Tom"},
        ),
        (
                {"names": ["Robert", "Jhon", "Petro", "Genri"], "movies": ["Iron man", "Tor", "Star Wars"]},
                {'query': 'Genri'},
                {"status": 200, "person": "Genri"},
        ),
        (
                {"names": ["Tom", "Jhon", "Sara", "Jeni"], "movies": ["Tank", "Zima", "Star Wars"]},
                {'query': 'Jeni'},
                {"status": 200, "person": "Jeni"},
        ),
    ]
)
@pytest.mark.anyio
async def test_person_films(data_for_write, query_data, expected_answer, es_write_data, http_request, es_delete_index_film, gen_data_person_tests, reset_redis): 
    data: dict = await gen_data_person_tests(data_for_write)
    await es_write_data(data=data.get("movies"), index_name=test_settings_movies.es_index, index_mapping=test_settings_movies.es_index_mapping)
    await es_write_data(data=data.get("persons"), index_name=test_settings_person.es_index, index_mapping=test_settings_person.es_index_mapping)
    uuid_person = data['persons_'][query_data['query']]
    modify_query = {'query': uuid_person}
    await asyncio.sleep(1)
    response = await http_request(modify_query, '/api/v1/persons/{}/films'.format(uuid_person))
    assert response['status'] == expected_answer['status']
    assert [film['title'] for film in response['body']] == data_for_write['movies']

    await es_delete_index_film(index_name=test_settings_movies.es_index)
    await es_delete_index_film(index_name=test_settings_person.es_index)

    response = await http_request(modify_query, '/api/v1/persons/{}/films'.format(uuid_person))
    assert response['status'] == expected_answer['status']
    assert [film['title'] for film in response['body']] == data_for_write['movies']

    await reset_redis() 