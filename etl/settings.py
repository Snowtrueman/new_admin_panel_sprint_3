import os
import datetime

from dotenv import load_dotenv


load_dotenv()

# COMMON

DB_QUERY_CHUNK_SIZE = 1
EPOCH_START_DATE = datetime.datetime(1970, 6, 24, 0, 0, 0)  # extracting start date

# POSTGRES

POSTGRES_DSN = {
    "dbname": os.environ.get("POSTGRES_DBNAME", "postgres"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": os.environ.get("POSTGRES_PORT", 5432)}

POSTGRES_SCHEMA = "content"

# ELASTIC

ELASTIC_DSN = (f"{os.environ.get('ELASTIC_SCHEMA', 'http')}://{os.environ.get('ELASTIC_HOST', '127.0.0.1')}:"
               f"{os.environ.get('ELASTIC_PORT', '9200')}")
ELASTIC_SCHEMA = "http"
ELASTIC_HOST = "127.0.0.1"
ELASTIC_PORT = 9200
ELASTIC_INDEX_NAME = "movies"
