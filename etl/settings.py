import os
import datetime

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PastDate


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=f"{os.path.dirname(os.path.abspath(__file__))}/.env")

    # COMMON

    DB_QUERY_CHUNK_SIZE: int = 500

    # extracting start date
    EPOCH_START_DATE: PastDate = datetime.datetime(1970, 6, 24, 0, 0, 0)

    # POSTGRES

    POSTGRES_DBNAME: str = "postgres"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_SCHEMA: str = "content"

    # ELASTIC

    ELASTIC_PROTOCOL: str = "http"
    ELASTIC_HOST: str = "127.0.0.1"
    ELASTIC_PORT: int = 9200
    ELASTIC_INDEX_NAME: str = "movies"
    ELASTIC_SCHEMA: str = "elastic_schema.json"

    # REDIS

    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_BASIC_STORAGE_KEY: str = "etl"


app_settings = AppSettings()




