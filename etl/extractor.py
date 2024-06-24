import logging
from typing import Optional
from contextlib import contextmanager
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from constants import Entities


class Extractor:
    """

    """

    def __init__(self, postgres_dns: dict, postgres_schema: str, chunk_size: str, logger: logging.Logger):
        self._postgres_dns: dict = postgres_dns
        self._postgres_schema: str = postgres_schema
        self._query_chunk_size = chunk_size
        self._logger: logging.Logger = logger

    @staticmethod
    @contextmanager
    def _get_db_connection(dsn: dict):
        """
        """

        conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)
        try:
            yield conn
        finally:
            conn.close()

    def _perform_db_action(self, query: str) -> list:
        """
        """

        with self._get_db_connection(self._postgres_dns) as conn:
            result = []
            curs = conn.cursor()
            offset = 0
            while True:
                curs.execute(query.format(offset))
                entities_changed = list(curs.fetchall())
                if not entities_changed:
                    break
                result.extend(entities_changed)
                offset += self._query_chunk_size
            return result

    def _preload_changed_entities(self, entity: Entities, start_date: datetime) -> Optional[list]:
        """
        Loads changed entities according to entity type and receives film work ids
        """

        entity_changed_query = (f"SELECT id "
                                f"FROM {self._postgres_schema}.{entity} "
                                f"WHERE modified > '{start_date}' "
                                f"ORDER BY modified "
                                f"LIMIT {self._query_chunk_size} OFFSET {{}};")

        try:
            self._logger.debug(f"Checking {entity} for changes")
            entities_changed = self._perform_db_action(entity_changed_query)
            self._logger.debug(f"Found {len(entities_changed)} changes in {entity}")
        except psycopg2.Error:
            self._logger.error("An error while interaction with Postgres")
        else:
            if entities_changed:
                filmworks_changed_query = (f"SELECT fw.id "
                                           f"FROM {self._postgres_schema}.film_work fw "
                                           f"LEFT JOIN {self._postgres_schema}.{entity}_film_work efw "
                                           f"ON fw.id = efw.film_work_id "
                                           f"WHERE efw.{entity}_id IN ({','.join(['%s'] * len(entities_changed))}) "
                                           f"ORDER BY fw.modified "
                                           f"LIMIT {self._query_chunk_size}")

                try:
                    filmworks_changed = self._perform_db_action(filmworks_changed_query)
                    self._logger.debug(f"Found {len(filmworks_changed)} changed film works "
                                       f"according to changes in {entity}")
                except psycopg2.Error as e:
                    self._logger.error("An error while interaction with Postgres")
                else:
                    result = self._extract_changed_filmworks(filmworks_changed)
                    return result

    def _extract_changed_filmworks(self,
                                   target_filmworks: Optional[list] = None,
                                   start_date: Optional[datetime] = None
                                   ) -> list:
        """
        Loads changed entities according to entity type and receives film work ids
        """

        extra_info_query = (f"SELECT fw.id as fw_id, fw.title, fw.description, fw.rating, fw.type, fw.created, "
                            f"fw.modified, pfw.role, p.id, p.full_name, g.name "
                            f"FROM content.film_work fw "
                            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
                            f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
                            f"LEFT JOIN content.genre g ON g.id = gfw.genre_id ")

        if target_filmworks:
            extra_info_query_case = f"WHERE fw.id IN ({','.join(['%s'] * len(target_filmworks))}); "
        else:
            extra_info_query_case = f"WHERE fw.modified > '{start_date}'; "

        extra_info_query += extra_info_query_case

        try:
            filmworks_changed = self._perform_db_action(extra_info_query)
            self._logger.debug(f"Extracted {len(filmworks_changed)} fimwork records affecting changes ")
            return filmworks_changed
        except psycopg2.Error:
            self._logger.error("An error while interaction with Postgres")

    def extract_data(self, start_date: Optional[datetime]):
        """
        Performs complex database check
        """

        for entity in [e.value for e in Entities]:
            filmworks_changed = self._preload_changed_entities(entity=entity, start_date=datetime.now())
        filmworks_changed = self._extract_changed_filmworks(start_date=datetime.now())
