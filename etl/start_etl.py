import logging
import time
import datetime
import subprocess

from settings import app_settings
from pipelines.pipeline import Pipeline
from clients.redis_client import RedisClient
from common.state_handler import RedisStorage, State


def get_and_configure_logger() -> logging.Logger:
    """
    Configures the logger instance
    """

    logger_format = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logger_format)

    # Turning off the third party modules loggers
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name != __name__:
            log_obj.disabled = True

    return logging.getLogger(__name__)


if __name__ == "__main__":
    app_logger = get_and_configure_logger()

    redis_client = RedisClient(app_settings.REDIS_HOST, app_settings.REDIS_PORT, app_logger)

    storage = RedisStorage(redis_client)
    state_handler = State(storage)

    POSTGRES_DSN: dict[str, str] = {
        "dbname": app_settings.POSTGRES_DBNAME,
        "user": app_settings.POSTGRES_USER,
        "password": app_settings.POSTGRES_PASSWORD,
        "host": app_settings.POSTGRES_HOST,
        "port": app_settings.POSTGRES_PORT
    }

    ELASTIC_DSN = f"{app_settings.ELASTIC_PROTOCOL}://{app_settings.ELASTIC_HOST}:{app_settings.ELASTIC_PORT}"

    pipeline = Pipeline(
        POSTGRES_DSN,
        app_settings.POSTGRES_SCHEMA,
        app_settings.DB_QUERY_CHUNK_SIZE,
        ELASTIC_DSN,
        app_settings.ELASTIC_INDEX_NAME,
        app_logger,
        state_handler
    )

    last_updated = state_handler.get_state("film_works_updated")

    start_date = last_updated if last_updated else app_settings.EPOCH_START_DATE

    while True:
        app_logger.debug("Starting ETL process")
        try:
            pipeline_started_at = datetime.datetime.utcnow()
            state_handler.set_state("film_works_updated", pipeline_started_at.isoformat())
            pipeline.do_etl_pipeline(start_date=start_date)
            time.sleep(10)
            start_date = pipeline_started_at
        except (SystemExit, KeyboardInterrupt) as e:
            app_logger.debug("ETL Process stopped")
