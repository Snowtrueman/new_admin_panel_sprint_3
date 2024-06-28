import logging

from pydantic import ValidationError

from serializers import PostgresFilmWorkSerializer, ElasticFilmWorkSerializer


class FilmWorkMerger:
    """
    Class for merging different film work records extracted from database into one (with merged genres and participants)
    """

    def __init__(self, income_film_works: list[dict], logger: logging.Logger):
        self._income_film_works: list[dict] = income_film_works
        self._merged_film_works: dict[str, PostgresFilmWorkSerializer] = {}
        self._logger: logging.Logger = logger

    def merge_persons(self) -> dict[str, PostgresFilmWorkSerializer]:
        """
        Merges different film work records extracted from database into one
        """

        for film_work_record in self._income_film_works:
            try:
                validated_film_work = PostgresFilmWorkSerializer(**film_work_record)
            except ValidationError:
                film_work_record_id = film_work_record.get("film_work_id")
                self._logger.error(f"An error in serializing extracted form Postgres "
                                   f"film work record with ID {film_work_record_id} ")
            else:
                person_role = validated_film_work.role
                person_name = validated_film_work.full_name
                person_id = validated_film_work.person_id
                genre = validated_film_work.genres
                if all([person_role, person_name, person_id, genre]):
                    if film_work_record["film_work_id"] not in self._merged_film_works:
                        setattr(validated_film_work, f"{person_role}_names", [person_name])
                        setattr(validated_film_work, f"{person_role}", [{"id": person_id, "name": person_name}])
                        setattr(validated_film_work, "genres_list", [genre])
                        self._merged_film_works[film_work_record["film_work_id"]] = validated_film_work
                    else:
                        existing_film_work = self._merged_film_works[film_work_record["film_work_id"]]
                        genres_list = getattr(existing_film_work, "genres_list")
                        persons_roles_list = getattr(existing_film_work, f"{person_role}_names")
                        persons_roles_list_extended = getattr(existing_film_work, f"{person_role}")
                        if person_name not in persons_roles_list:
                            persons_roles_list.append(person_name)
                        if genre not in genres_list:
                            genres_list.append(genre)
                        persons_ids = [item[key] for item in persons_roles_list_extended
                                       for key, value in item.items() if key == "id"]
                        if person_id not in persons_ids:
                            persons_roles_list_extended.append({"id": person_id, "name": person_name})
                else:
                    self._merged_film_works[film_work_record["film_work_id"]] = validated_film_work
        return self._merged_film_works


class PostgresToElasticTransformer:
    """
    Class for serializing merged film work objects to Elasticsearch index format
    """

    def __init__(self, postgres_film_works_list: dict[str, PostgresFilmWorkSerializer], logger: logging.Logger):
        self._postgres_film_works_list: dict[str, PostgresFilmWorkSerializer] = postgres_film_works_list
        self._elastic_film_works_list:  list[ElasticFilmWorkSerializer] = []
        self._logger: logging.Logger = logger

    def transform(self) -> list[ElasticFilmWorkSerializer]:
        """
        Transforms film work objects to Elasticsearch index format
        """

        for film_work_id, film_work_data in self._postgres_film_works_list.items():
            try:
                elastic_film_work = ElasticFilmWorkSerializer(
                    id=film_work_data.film_work_id,
                    imdb_rating=film_work_data.rating,
                    genres=film_work_data.genres_list,
                    title=film_work_data.title,
                    description=film_work_data.description,
                    directors_names=film_work_data.director_names,
                    actors_names=film_work_data.actor_names,
                    writers_names=film_work_data.writer_names,
                    directors=film_work_data.director,
                    actors=film_work_data.actor,
                    writers=film_work_data.writer
                )
                self._elastic_film_works_list.append(elastic_film_work)
            except ValidationError:
                self._logger.error(f"An error in serializing extracted form Postgres "
                                   f"film work record with ID {film_work_id} ")
        return self._elastic_film_works_list
