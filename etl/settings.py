import os
import datetime

from dotenv import load_dotenv


load_dotenv()

# COMMON

POSTMAN_TESTS_PATH = "etl_tests.json"
DB_QUERY_CHUNK_SIZE = 500
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
ELASTIC_INDEX_NAME = "movies"
ELASTIC_SCHEMA = "elastic_schema.json"

# REDIS

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_BASIC_STORAGE_KEY = os.environ.get("REDIS_BASIC_STORAGE_KEY", "etl")
