import pytest
import asyncio
from ...testdata.film import GenreRequest
from pydantic import error_wrappers
from ...settings import test_settings_genre
from .conftest import genres
from http import HTTPStatus


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'page_size': 50},
                {'status': HTTPStatus.OK, 'length': 26}
        ),
        (
                {'page_size': 1},
                {'status': HTTPStatus.OK, 'length': 1}
        )
    ]
)
@pytest.mark.anyio
async def test_all_genres(
        query_data, expected_answer, es_write_data,
        generate_genres, es_delete_index_film, http_request):
    '''тест вывода всех жанров'''
    es_data: list = await generate_genres(num_documents=expected_answer['length'])
    await es_write_data(data=es_data, index_name=test_settings_genre.es_index,
                        index_mapping=test_settings_genre.es_index_mapping)
    await asyncio.sleep(1)

    response = await http_request(query_data, '/api/v1/genres')
    assert response['status'] == expected_answer['status']
    assert len(response['body']) == expected_answer['length']

    await es_delete_index_film(test_settings_genre.es_index)

    response = await http_request(query_data, '/api/v1/genres')
    assert response['status'] == expected_answer['status']
    assert len(response['body']) == expected_answer['length']


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'genre_id': '000'},
                {'status': HTTPStatus.OK, 'length': 1}
        ),
        (
                {'genre_id': '555'},
                {'status': HTTPStatus.OK, 'length': 1}),
    ]
)
@pytest.mark.anyio
async def test_genres_by_id(
        es_write_data, generate_genres, es_delete_index_film,
        http_request, query_data, expected_answer):
    '''тест вывода жанра по id'''
    es_data: list = await generate_genres(num_documents=10)
    await es_write_data(data=es_data, index_name=test_settings_genre.es_index,
                        index_mapping=test_settings_genre.es_index_mapping)
    await asyncio.sleep(1)

    genre_id = query_data['genre_id']
    response = await http_request(query_data, f'/api/v1/genres/{genre_id}')
    assert response['status'] == expected_answer['status']
    genre_num = int(genre_id[0])
    genre = response['body']
    assert genre['name'] == genres[genre_num]

    await es_delete_index_film(test_settings_genre.es_index)

    response = await http_request(query_data, f'/api/v1/genres/{genre_id}')
    assert response['status'] == expected_answer['status']


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'genre_id': '8585'},
                {'status': HTTPStatus.NOT_FOUND, 'length': 1}
        )
    ]
)
@pytest.mark.anyio
async def test_genres_by_wrong_id(
        es_write_data, generate_genres, es_delete_index_film,
        http_request, query_data, expected_answer):
    '''тест вывода жанра по id'''
    es_data: list = await generate_genres(num_documents=10)
    await es_write_data(data=es_data, index_name=test_settings_genre.es_index,
                        index_mapping=test_settings_genre.es_index_mapping)
    await asyncio.sleep(1)
    genre_id = query_data['genre_id']
    response = await http_request(query_data, f'/api/v1/genres/{genre_id}')
    assert response['status'] == expected_answer['status']


@pytest.mark.anyio
async def test_validate_data(es_write_data, generate_genres, http_request):
    '''тест проверяет корректность валидации данных, используя модуль Pydantic'''
    es_data = await generate_genres(num_documents=1)
    await es_write_data(data=es_data, index_name=test_settings_genre.es_index,
                        index_mapping=test_settings_genre.es_index_mapping)
    await asyncio.sleep(1)
    response = await http_request({}, '/api/v1/genres')
    assert response['status'] == HTTPStatus.OK
    genre = response['body'][0]
    try:
        GenreRequest(**genre)
    except error_wrappers.ValidationError as error:
        raise ValueError(f"Ошибка валидации фильма: {error}")
    else:
        assert True
        
