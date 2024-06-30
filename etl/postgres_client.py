import logging
from typing import Optional, Union
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection

from common import backoff


class PostgresClient:
    """
    Client class for interacting with Postgres
    """

    def __init__(self, postgres_dns: dict, postgres_schema: str, chunk_size: int, logger: logging.Logger):
        self._postgres_dsn: dict = postgres_dns
        self._postgres_schema: str = postgres_schema
        self._query_chunk_size: int = chunk_size
        self._logger: logging.Logger = logger

    @property
    def logger(self):
        return self._logger

    @contextmanager
    def __get_db_connection(self) -> Optional[connection]:
        """
        Yields Postgres connection instance
        """

        try:
            conn = psycopg2.connect(**self._postgres_dsn, cursor_factory=DictCursor)
        except psycopg2.Error as exception:
            self._logger.error(f"Can't establish connection to Postgres "
                               f"with provided credentials: {self._postgres_dsn}")
            raise exception
        else:
            try:
                yield conn
            except psycopg2.Error as exception:
                self._logger.error("An error while trying to connect to Postgres")
                conn.close()
                raise exception

    def __make_dicts_from_query(self, columns: list, query_result: list) -> Optional[list[dict]]:
        """
        Makes list of dicts from query results
        """

        if not all([columns, query_result]):
            self._logger.error("Can't transform query results into dict: column names or query results is missing")
            return

        results_as_dicts = []
        for row in query_result:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col.name] = row[i]
            results_as_dicts.append(row_dict)
        return results_as_dicts

    @backoff()
    def perform_db_action(self, query: str, items_ids: Optional[list] = None, flat: bool = True ) -> Union[list, dict]:
        """
        Executes query in Postgres
        """

        with self.__get_db_connection(self._postgres_dsn) as conn:
            query_result = []
            curs = conn.cursor()
            offset = 0
            while True:
                if items_ids:
                    curs.execute(query.format(offset), items_ids)
                else:
                    curs.execute(query.format(offset))
                chunk_results = list(curs.fetchall())
                if not chunk_results:
                    break
                if flat:
                    query_result.extend(*chunk_results)
                else:
                    query_result.extend(chunk_results)
                offset += self._query_chunk_size
            if flat:
                return query_result
            else:
                columns = list(curs.description)
                return self.__make_dicts_from_query(columns, query_result)
