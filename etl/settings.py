import os
import datetime

from dotenv import load_dotenv


load_dotenv()

POSTGRES_DSN = {
    "dbname": os.environ.get("POSTGRES_DBNAME", "postgres"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": os.environ.get("POSTGRES_PORT", 5432)}

POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA", "content")

DB_QUERY_CHUNK_SIZE = 1

EPOCH_START_DATE = datetime.datetime(1970, 1, 1, 0, 0, 0)
