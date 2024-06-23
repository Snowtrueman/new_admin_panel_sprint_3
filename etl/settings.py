import os

from dotenv import load_dotenv


load_dotenv()

POSTGRES_DSN = {
    "dbname": os.environ.get("POSTGRES_DBNAME", "postgres"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": os.environ.get("POSTGRES_PORT", 5432)}

DB_QUERY_CHUNK_SIZE = 100
