from contextlib import contextmanager
from pathlib import Path
import logging
import os

import sqlite3
import psycopg2
from psycopg2.extras import DictCursor


@contextmanager
def sqlite_conn_context(db_path: str, logger: logging.Logger):
    db_path = Path(db_path)
    script_path = Path(__file__).parent.resolve()
    full_path = Path(script_path, db_path)
    if not full_path.is_file():
        logger.critical("Provided path to initial database name is not valid")
        return
    conn = sqlite3.connect(full_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def pg_conn_context(dsn: dict):
    conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)
    try:
        yield conn
    finally:
        conn.close()
