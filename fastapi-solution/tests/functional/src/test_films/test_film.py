import pytest
import time


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The star', 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'Mashed'},
                {'status': 200, 'length': 0}
        )
    ]
)
@pytest.mark.anyio
async def test_search(query_data, expected_answer, es_write_data, generate_films, http_request):
    es_data = await generate_films(num_documents=60, title='The star')
    await es_write_data(es_data)
    time.sleep(1)
    response = await http_request(query_data, '/api/v1/films/search')
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
    es_data: list = await generate_films(num_documents=60)
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
                {'genre': '2dasd31', 'page_size': 50},
                {'status': 200, 'length': 20}
        ),
        (
                {'genre': '2dasd31234', 'page_size': 50},
                {'status': 200, 'length': 20}
        )
    ]
)
@pytest.mark.anyio
async def test_all_films_filter(query_data, expected_answer, es_write_data, generate_films_filter_genre, generate_films, http_request):
    es_data: list = await generate_films_filter_genre(num_documents=expected_answer['length'], genre_id=query_data['genre'])
    es_data.extend(await generate_films(num_documents=20))
    await es_write_data(es_data)
    time.sleep(1)
    response = await http_request(query_data, '/api/v1/films')
    assert response['status'] == expected_answer['status']
    assert len(response['body']['result']) == expected_answer['length']


