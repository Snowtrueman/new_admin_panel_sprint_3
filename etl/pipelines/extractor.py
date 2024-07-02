import logging
from typing import Optional
from datetime import datetime

from common.constants import Entities
from clients.postgres_client import PostgresClient


class Extractor:
    """
    Class for extracting entities from database
    """

    def __init__(self, postgres_client: PostgresClient, postgres_schema: str, chunk_size: int, logger: logging.Logger):
        self.__postgres_schema: str = postgres_schema
        self.__query_chunk_size: int = chunk_size
        self.__logger: logging.Logger = logger
        self.__postgres_client: PostgresClient = postgres_client

    def find_changes_in_related_entities(self, entity: Entities, start_date: datetime) -> Optional[list]:
        """
        Loads changed entities according to entity type and receives film work ids
        """

        entity_changed_query = (f"SELECT id "
                                f"FROM {self.__postgres_schema}.{entity} "
                                f"WHERE modified > '{start_date}' "
                                f"ORDER BY modified "
                                f"LIMIT {self.__query_chunk_size} OFFSET {{}};")

        self.__logger.debug(f"Checking {entity} for changes")
        entities_changed = self.__postgres_client.perform_db_action(entity_changed_query)
        self.__logger.debug(f"Found {len(entities_changed)} changes in {entity}")

        if entities_changed:
            film_works_changed_query = (f"SELECT fw.id "
                                        f"FROM {self.__postgres_schema}.film_work fw "
                                        f"LEFT JOIN {self.__postgres_schema}.{entity}_film_work efw "
                                        f"ON fw.id = efw.film_work_id "
                                        f"WHERE efw.{entity}_id IN ({','.join(['%s'] * len(entities_changed))}) "
                                        f"ORDER BY fw.modified "
                                        f"LIMIT {self.__query_chunk_size} OFFSET {{}};")
            film_works_changed = self.__postgres_client.perform_db_action(film_works_changed_query, entities_changed)
            self.__logger.debug(f"Found {len(film_works_changed)} changed film works "
                                f"according to changes in {entity}")
            film_works_changed_records = self.extract_changed_film_works(start_date, film_works_changed)
            return film_works_changed_records

    def extract_changed_film_works(self, start_date: datetime, target_film_works: Optional[list] = None) -> list:
        """
        Loads changed entities according to entity type and receives film work ids
        """

        film_work_query = (f"SELECT fw.id as film_work_id, fw.title, fw.description, fw.rating, fw.type, "
                           f"pfw.role, p.id as person_id, p.full_name, g.name as genres "
                           f"FROM content.film_work fw "
                           f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
                           f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                           f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
                           f"LEFT JOIN content.genre g ON g.id = "
                           f"gfw.genre_id ")

        if target_film_works:
            film_work_query_case = (f"WHERE fw.id IN ({','.join(['%s'] * len(target_film_works))}) "
                                    f"LIMIT {self.__query_chunk_size} OFFSET {{}}; ")
        else:
            film_work_query_case = (f"WHERE fw.modified > '{start_date}' "
                                    f"LIMIT {self.__query_chunk_size} OFFSET {{}}; ")

        film_work_query += film_work_query_case
        film_works_changed = self.__postgres_client.perform_db_action(query=film_work_query,
                                                                      items_ids=target_film_works,
                                                                      flat=False)
        if film_works_changed:
            self.__logger.debug(f"Extracted {len(film_works_changed)} film work records affecting changes ")

        return film_works_changed
