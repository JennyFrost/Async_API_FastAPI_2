import pytest
import time
from ...testdata.film import Film, BaseFilmRequest
import pydantic


@pytest.mark.parametrize(
    'query_data, expected_answer, reset_redis_flag',
    [
        (
                {'query': 'The star', 'page_size': 50},
                {'status': 200, 'length': 50},
                True
        ),
        (
                {'query': 'Mashed'},
                {'status': 200, 'length': 1},
                True
        ),
        (
                {'query': 'Mashed'},
                {'status': 200, 'length': 2},
                False
        ),
        (
                {'query': 'Mashed'},
                {'status': 200, 'length': 2},
                True
        )
    ]
)
@pytest.mark.anyio
async def test_search(query_data, expected_answer, reset_redis_flag, es_write_data, generate_films, http_request, reset_redis, es_delete_index_film):
    es_data = await generate_films(
        num_documents=expected_answer['length'],
        title=query_data['query'])
    es_data.extend(await generate_films(num_documents=60))
    await es_write_data(data=es_data)
    time.sleep(1)
    response = await http_request(query_data, '/api/v1/films/search')
    if reset_redis_flag:
        await reset_redis()
    else:
        await es_delete_index_film()
    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'sort': '-imdb_rating'},
                {'status': 200}
        ),
        (
                {'sort': 'imdb_rating'},
                {'status': 200}
        )
    ]
)
@pytest.mark.anyio
async def test_all_films_sort(query_data, expected_answer, es_write_data, generate_films, http_request):
    es_data = await generate_films(num_documents=60)
    await es_write_data(es_data)
    time.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    first_rating = response['body']['result'][0]['imdb_rating']
    last_rating = response['body']['result'][-1]['imdb_rating']
    print(first_rating, last_rating)
    if query_data['sort'].startswith('-'):
        assert first_rating > last_rating
    else:
        assert first_rating < last_rating


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'genre': '123fsgdh', 'page_size': 50},
                {'status': 200, 'length': 20}
        ),
        (
                {'genre': '4588dfdjjh', 'page_size': 50},
                {'status': 200, 'length': 20}
        )
    ]
)
@pytest.mark.anyio
async def test_all_films_filter(
        query_data, expected_answer, es_write_data,
        generate_films_filter_genre, generate_films, http_request):
    es_data: list = await generate_films_filter_genre(
        num_documents=expected_answer['length'], genre_id=query_data['genre'])
    es_data.extend(await generate_films(num_documents=20))
    await es_write_data(es_data)
    time.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']


@pytest.mark.anyio
async def test_validate_data(es_write_data, generate_films, http_request):
    '''тест проверяет корректность валидации данных, используя модуль Pydantic'''
    es_data = await generate_films(num_documents=1)
    await es_write_data(es_data)
    time.sleep(1)
    response = await http_request({}, '/api/v1/films')
    assert response['status'] == 200
    film = response['body']['result'][0]
    try:
        BaseFilmRequest(**film)
    except pydantic.error_wrappers.ValidationError as error:
        raise ValueError(f"Ошибка валидации фильма: {error}")
    else:
        assert True
