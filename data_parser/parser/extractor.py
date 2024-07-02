import csv
import logging
from pathlib import Path
from typing import Optional

import sqlite3
from sqlite3 import Connection as sqlite_connection

from settings import EXPORT_FILES_DIR, CHUNK_SIZE


class SQLiteExtractor:
    """
    Loads data from SQLite DB according to provided target tables and its fields
    """

    def __init__(self, db_connection: sqlite_connection, tables_to_export: tuple[str, ...],
                 fields_mapper: dict[str, tuple[str, ...]], logger: logging.Logger):
        self._db_connection: sqlite_connection = db_connection
        self._logger: logging.Logger = logger
        self._target_tables: tuple[str, ...] = tables_to_export
        self._table_fields_mapper: dict[str, tuple[str, ...]] = fields_mapper
        self._fields: Optional[tuple] = None

    def _compile_export_query(self, db_table: str) -> Optional[str]:
        """
        Generate SQL query string for data extraction
        """

        query = f"SELECT {('{}, ' * len(self._fields))[:-2]} FROM {db_table};"
        return query.format(*self._fields)

    @staticmethod
    def _create_files_dir():
        """
        Creates directory for csv file storage
        """

        target_path = Path(EXPORT_FILES_DIR)
        target_path.mkdir(parents=True, exist_ok=True)

    def extract_movies(self) -> None:
        """
        Perform data export
        """

        with self._db_connection as conn:
            curs = conn.cursor()
            for db_table in self._target_tables:
                self._fields = self._table_fields_mapper.get(db_table)
                if not self._fields:
                    self._logger.error(f"Error while trying to compile export query for table {db_table}")
                    continue
                export_query = self._compile_export_query(db_table)
                if not export_query:
                    continue
                try:
                    curs.execute(export_query)
                except sqlite3.Error as e:
                    self._logger.error(f"Error while trying to load data from initial database: {e}")
                    continue
                else:
                    self._create_files_dir()
                    with open(f"{EXPORT_FILES_DIR}/{db_table}.csv", "w") as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=self._fields)
                        while table_content := curs.fetchmany(CHUNK_SIZE):
                            for row in table_content:
                                writer.writerow(dict(row))
