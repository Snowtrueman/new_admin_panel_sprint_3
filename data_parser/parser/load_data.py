import logging
import os

from psycopg2.extensions import connection as pg_connection
from psycopg2 import OperationalError
from sqlite3 import Connection as sqlite_connection

from extractor import SQLiteExtractor
from saver import PostgresSaver
from common import sqlite_conn_context, pg_conn_context
from settings import TABLES_TO_EXPORT, EXPORT_TABLE_FIELDS_MAPPER, SQLITE_FILE, POSTGRES_DSN


def load_from_sqlite(sqlite_conn: sqlite_connection, pg_conn: pg_connection, logger):
    """
    Main entrypoint
    """

    sqlite_extractor = SQLiteExtractor(sqlite_conn, TABLES_TO_EXPORT, EXPORT_TABLE_FIELDS_MAPPER, logger)
    postgres_saver = PostgresSaver(pg_conn, TABLES_TO_EXPORT, logger)

    sqlite_extractor.extract_movies()
    postgres_saver.save_all_data()
    os.environ["PARSING_STATUS"] = "OK"



if __name__ == "__main__":
    logger_format = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logger_format)
    app_logger = logging.getLogger()

    try:
        with (sqlite_conn_context(SQLITE_FILE, app_logger) as sqlite_context,
              pg_conn_context(POSTGRES_DSN) as pg_context):
            if all([sqlite_context, pg_context]):
                load_from_sqlite(sqlite_context, pg_context, app_logger)
    except OperationalError:
        app_logger.debug(POSTGRES_DSN)
        app_logger.critical("Can not connect to Postgres with provided credentials")
