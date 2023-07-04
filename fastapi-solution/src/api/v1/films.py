from http import HTTPStatus
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query

from services.film import FilmService, get_film_service

from .api_models import Film, Genre, PersonBase, PageAnswer, FilmBase as FilmAnswer
from models.film import FilmBase
from core.config import settings

PAGE_SIZE = settings.page_size
SORT_FIELD = settings.sort_field

router = APIRouter()


@router.get('/search', response_model=PageAnswer)
async def query_films(query: str,
                      page_number: Annotated[int, Query(description='Pagination page number', ge=1)] = 1,
                      page_size: Annotated[int, Query(description='Pagination page size', ge=1)] = PAGE_SIZE,
                      film_service: FilmService = Depends(get_film_service)) -> PageAnswer:
    """
    Метод возвращает список фильмов по поисковому запросу 

     - **query**: параметр поиска, поиск производится по названию фильма
    """
    films: list[FilmBase] = await film_service.get_films_query(page_number, page_size, query)
    page_model = PageAnswer(
        page_size=page_size,
        number_page=page_number,
        amount_elements=len(films),
        result=[FilmAnswer.parse_obj(film_obj.dict()) for film_obj in films]
    )
    return page_model


@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> Film:
    """
    Метод возвращает информацию об одном фильме по **film_id**
    """
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return Film(
        uuid=film.id,
        title=film.title,
        imdb_rating=film.imdb_rating,
        description=film.description,
        creation_date=film.creation_date,
        actors=[PersonBase(full_name=i.name, uuid=i.id) for i in film.actors],
        writers=[PersonBase(full_name=i.name, uuid=i.id) for i in film.writers],
        directors=[PersonBase(full_name=i.name, uuid=i.id) for i in film.director],
        genre=[Genre(name=i.name, uuid=i.id) for i in film.genre],
    )


@router.get('/', response_model=PageAnswer)
async def all_films(page_number: Annotated[int, Query(description='Pagination page number', ge=1)] = 1,
                     page_size: Annotated[int, Query(description='Pagination page size', ge=1)] = PAGE_SIZE,
                    sort: Annotated[str, Query(regex="^[-]?imdb_rating$")] = SORT_FIELD,
                    genre: str = None, film_service: FilmService= Depends(get_film_service)) -> PageAnswer:
    """
    Метод возвращает список фильмов

     - **sort**: параметр сортировки, по умолчанию сортировка идет по полю **rating** по убыванию
     - **genre**: опционально можно отфильтровать фильмы по полю жанры 
    """
    films: list[FilmBase] = await film_service.get_films_page(page_number, page_size, sort, genre)
    page_model = PageAnswer(
        page_size=page_size,
        number_page=page_number,
        amount_elements=len(films),
        result=[FilmAnswer.parse_obj(film_obj.dict()) for film_obj in films]
    )
    return page_model
