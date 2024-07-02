import csv
import logging
from pathlib import Path
from typing import Optional
from dataclasses import astuple
import dataclasses
import shutil

import psycopg2
from psycopg2.extensions import connection as pg_connection, cursor

from settings import EXPORT_FILES_DIR, POSTGRES_SCHEMA, EXPORT_TABLE_FIELDS_MAPPER, CHUNK_SIZE
from parser import DataParser


class PostgresSaver:
    """
    Saves data to Postgres according to provided target tables and its fields
    """

    def __init__(self, db_connection: pg_connection, tables_to_export: tuple[str, ...], logger: logging.Logger):
        self._db_connection: pg_connection = db_connection
        self._tables_to_export: tuple[str, ...] = tables_to_export
        self._logger: logging.Logger = logger

    def _check_all_data_files_exists(self) -> Optional[bool]:
        """
        Checks for csv files existence according to tables list
        """

        for table in self._tables_to_export:
            db_path = Path(f"{EXPORT_FILES_DIR}/{table}.csv")
            if not db_path.is_file():
                self._logger.critical("Error in data saving to postgres - some data files are missing")
                return
        return True



    def _check_all_tables_exists(self):
        """
        Checks the existence of all required tables in the DB
        """

        exists = False
        try:
            with self._db_connection as conn:
                curs = conn.cursor()
                for table in self._tables_to_export:
                    curs.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                                 f"WHERE table_schema = '{POSTGRES_SCHEMA}' AND table_name = '{table}');")
                    exists = curs.fetchone()[0]
        except psycopg2.Error as e:
            self._logger.error(f"DB error while trying to check the existence of necessary tables in Postgres: {e}")
        return exists

    def _check_files(self) -> Optional[bool]:
        """
        Checks for export files existence
        """

        files = self._check_all_data_files_exists()
        if not files:
            self._logger.error("One or more export files are missing")
            return
        return True

    def _check_tables(self) -> Optional[bool]:
        """
        Checks for export files existence
        """

        tables = self._check_all_tables_exists()
        if not tables:
            self._logger.error("One or more required tables in Postgres are missing")
            return
        return True

    @staticmethod
    def _prepare_pg_query(serialized_data: list, pg_cursor: cursor, table: str) -> str:
        """
        Prepare SQL query for data import
        """

        column_names = [field.name for field in dataclasses.fields(serialized_data[0])]
        column_names_str = ",".join(column_names)
        col_count = ", ".join(["%s"] * len(column_names))
        bind_values = ",".join(pg_cursor.mogrify(f"({col_count})", astuple(item)).decode("utf-8")
                               for item in serialized_data)
        query = (f"INSERT INTO {POSTGRES_SCHEMA}.{table} ({column_names_str}) VALUES {bind_values} "
                 f"ON CONFLICT (id) DO NOTHING")
        return query

    def save_all_data(self):
        """
        Perform data import
        """

        if not all([self._check_files(), self._check_tables()]):
            return

        for table in self._tables_to_export:
            serializer = DataParser(table, self._logger)
            fields = EXPORT_TABLE_FIELDS_MAPPER.get(table)

            with open(f"{EXPORT_FILES_DIR}/{table}.csv") as csvfile:
                items_list = list(csv.DictReader(csvfile, fieldnames=fields))
                start = 0
                step = CHUNK_SIZE
                total_read = 0
                total_write = 0
                while items_list:
                    with self._db_connection as conn:
                        curs = conn.cursor()
                        _slice = slice(start, step)
                        extracted = items_list[_slice]
                        buff = [dict(row) for row in extracted]
                        total_read += len(buff)
                        del items_list[_slice]
                        serialized = serializer.parse_data(buff)

                        query = self._prepare_pg_query(serialized, curs, table)

                        try:
                            curs.execute(query)
                            conn.commit()
                            total_write += len(serialized)
                        except psycopg2.Error as e:
                            self._logger.error(f"An error while trying to import data to Postgres: {e}")
                self._logger.debug(f"Table {table} | "
                                   f"Exported items cnt: {total_read} | "
                                   f"Imported items cnt: {total_write}")
        shutil.rmtree(EXPORT_FILES_DIR)
