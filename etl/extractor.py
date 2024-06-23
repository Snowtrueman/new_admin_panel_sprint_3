import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor


class Extractor:
    """

    """

    def __init__(self, postgres_dns: dict, chunk_size: str, logger: logging.Logger):
        self._postgres_dns: dict = postgres_dns
        self._query_chunk_size = chunk_size
        self._logger: logging.Logger = logger

    @staticmethod
    @contextmanager
    def _get_db_connection(dsn: dict):
        conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)
        try:
            yield conn
        finally:
            conn.close()

    def do_persons_pipeline(self):
        """
        """

        persons_changed_query = (f"SELECT id "
                                 f"FROM content.person "
                                 f"WHERE modified > '2024-06-22' "
                                 f"ORDER BY modified "
                                 f"LIMIT {self._query_chunk_size};")

        with self._get_db_connection(self._postgres_dns) as conn:
            curs = conn.cursor()
            curs.execute(persons_changed_query)
            persons_changed = list(curs.fetchall())

        filmworks_changed_query = (f"SELECT fw.id "
                                   f"FROM content.film_work fw "
                                   f"LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id "
                                   f"WHERE pfw.person_id IN ({','.join(['%s'] * len(persons_changed))}) "
                                   f"ORDER BY fw.modified "
                                   f"LIMIT {self._query_chunk_size}")

        with self._get_db_connection(self._postgres_dns) as conn:
            curs = conn.cursor()
            curs.execute(filmworks_changed_query, *persons_changed)
            filmworks_changed = list(curs.fetchall())

        res = self.extract_changed_filmworks(filmworks_changed)
        a = 1

    def do_genre_pipeline(self):
        """
        """

        genres_changed_query = ("SELECT id, modified "
                                "FROM content.genre "
                                "WHERE modified > ? "
                                "ORDER BY modified "
                                "LIMIT ?;")

        filmworks_changed_query = (f"SELECT id, modifies "
                                   f"FROM content.filmworks fw "
                                   f"LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id "
                                   f"WHERE gfw.genre_id IN {changed_genre_ids} "
                                   f"ORDER BY fw.modified "
                                   f"LIMIT {self._query_chunk_size}")

    def extract_changed_filmworks(self, target_filmworks) -> list:
        """
        """

        extra_info_query = (f"SELECT fw.id as fw_id, fw.title, fw.description, fw.rating, fw.type, fw.created, "
                            f"fw.modified, pfw.role, p.id, p.full_name, g.name "
                            f"FROM content.film_work fw "
                            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
                            f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
                            f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                            f"WHERE fw.id IN ({','.join(['%s'] * len(target_filmworks))}); ")

        with self._get_db_connection(self._postgres_dns) as conn:
            curs = conn.cursor()
            curs.execute(extra_info_query, *target_filmworks)
            filmworks_changed = list(curs.fetchall())
            return filmworks_changed
