import pytest
import asyncio
from ...testdata.film import BaseFilmRequest, FilmRequest
import pydantic
from ...settings import test_settings_movies
from http import HTTPStatus


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The star', 'page_size': 50},
                {'status': HTTPStatus.OK, 'length': 50},
        ),
        (
                {'query': 'Mashed'},
                {'status': HTTPStatus.OK, 'length': 0},
        ),
        (
                {'query': 'Mashed'},
                {'status': HTTPStatus.OK, 'length': 2},
        )
    ]
)
@pytest.mark.anyio
async def test_search(
        query_data, expected_answer,
        es_write_data, generate_films, http_request, reset_redis, es_delete_index_film
):
    '''тест поиска фильмов'''
    es_data = await generate_films(
        num_documents=expected_answer['length'],
        title=query_data['query'])
    es_data.extend(await generate_films(num_documents=60))
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/films/search')

    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']

    await es_delete_index_film(index_name=test_settings_movies.es_index)

    response = await http_request(query_data, '/api/v1/films/search')

    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']

    await reset_redis()


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'sort': '-imdb_rating'},
                {'status': HTTPStatus.OK}
        ),
        (
                {'sort': 'imdb_rating'},
                {'status': HTTPStatus.OK}
        )
    ]
)
@pytest.mark.anyio
async def test_all_films_sort(
        query_data, expected_answer,
        es_write_data, generate_films, http_request, es_delete_index_film, reset_redis
):
    '''тест сортировки фильмов по рейтингу'''
    es_data = await generate_films(num_documents=60)
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    first_rating = response['body']['result'][0]['imdb_rating']
    last_rating = response['body']['result'][-1]['imdb_rating']
    if query_data['sort'].startswith('-'):
        assert first_rating > last_rating
    else:
        assert first_rating < last_rating

    await es_delete_index_film(test_settings_movies.es_index)

    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    first_rating = response['body']['result'][0]['imdb_rating']
    last_rating = response['body']['result'][-1]['imdb_rating']
    if query_data['sort'].startswith('-'):
        assert first_rating > last_rating
    else:
        assert first_rating < last_rating

    await reset_redis()


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'sort': 'im_rating'},
                {'status': HTTPStatus.UNPROCESSABLE_ENTITY}
        )
    ]
)
@pytest.mark.anyio
async def test_validate_sort_film(
        query_data, expected_answer,
        es_write_data, generate_films, http_request, es_delete_index_film, reset_redis
):
    '''тест проверка валидации параметров сортировки фильмов по рейтингу'''
    es_data = await generate_films(num_documents=60)
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']

    await es_delete_index_film(test_settings_movies.es_index)

    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']

    await reset_redis()


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'genre': '123fsgdh', 'page_size': 50},
                {'status': HTTPStatus.OK, 'length': 20}
        ),
        (
                {'genre': '4588dfdjjh', 'page_size': 50},
                {'status': HTTPStatus.OK, 'length': 20}
        )
    ]
)
@pytest.mark.anyio
async def test_all_films_filter(
        query_data, expected_answer, es_write_data,
        generate_films_filter_genre, generate_films, http_request, es_delete_index_film, reset_redis
):
    '''тест фильтрации фильмов по жанру'''
    es_data: list = await generate_films_filter_genre(
        num_documents=expected_answer['length'], genre_id=query_data['genre'])
    es_data.extend(await generate_films(num_documents=20))
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']

    await es_delete_index_film(test_settings_movies.es_index)

    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']

    await reset_redis()


@pytest.mark.anyio
async def test_validate_data(es_write_data, generate_films, http_request):
    '''тест проверяет корректность валидации данных, используя модуль Pydantic'''
    es_data = await generate_films(num_documents=1)
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request({}, '/api/v1/films')
    assert response['status'] == HTTPStatus.OK
    film = response['body']['result'][0]
    try:
        BaseFilmRequest(**film)
    except pydantic.error_wrappers.ValidationError as error:
        raise ValueError(f"Ошибка валидации фильма: {error}")
    else:
        assert True


@pytest.mark.parametrize(
    'input_data, output_data',
    [
        (
            {'film_id': 'e4e97d90-ac31-46bd-bed3-43bc58b75961'},
            {'find_id': 'e4e97d90-ac31-46bd-bed3-43bc58b75961', 'status': HTTPStatus.OK}
        ),
        (
            {'film_id': 'e4e97d90-ac31-46bd-bed3-43bc58b75961'},
            {'find_id': 'e7e97d90-ac31-46bd-bed3-43bc58b76745', 'status': HTTPStatus.OK}
        )
    ]
)
@pytest.mark.anyio
async def test_detail_film(
        input_data, output_data,
        generate_films, http_request, es_write_data, reset_redis
):
    await reset_redis()
    es_data: list = await generate_films(
        num_documents=1, film_id=input_data['film_id'])
    es_data.extend(await generate_films(num_documents=20))
    await es_write_data(data=es_data, index_name=test_settings_movies.es_index,
                        index_mapping=test_settings_movies.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request({}, f'/api/v1/films/{output_data["find_id"]}')
    assert response['status'] == output_data['status']
