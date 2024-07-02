from pydantic import BaseModel


class PostgresFilmWorkSerializer(BaseModel):
    """
    Model for serializing film work object extracted from Postgres
    """

    film_work_id: str
    title: str
    description: str
    rating: float
    type: str
    role: str | None = ""
    person_id: str | None = ""
    full_name: str | None = ""
    genres: str
    genres_list: list = []
    director_names: list = []
    actor_names: list = []
    writer_names: list = []
    director: list[dict[str, str]] = []
    actor: list[dict[str, str]] = []
    writer: list[dict[str, str]] = []


class ElasticFilmWorkSerializer(BaseModel):
    """
    Model for serializing film work object before uploading in Elasticsearch
    """

    id: str
    imdb_rating: float
    genres: list
    title: str
    description: str
    directors_names: list
    actors_names: list
    writers_names: list
    directors: list[dict[str, str]]
    actors: list[dict[str, str]]
    writers: list[dict[str, str]]
