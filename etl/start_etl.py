import logging
import time
import datetime
import subprocess

from pipelines.pipeline import Pipeline
from clients.redis_client import RedisClient
from common.state_handler import RedisStorage, State
from settings import (POSTGRES_DSN, POSTGRES_SCHEMA, DB_QUERY_CHUNK_SIZE, EPOCH_START_DATE,
                      ELASTIC_DSN, ELASTIC_INDEX_NAME, REDIS_HOST, REDIS_PORT, POSTMAN_TESTS_PATH)


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

    redis_client = RedisClient(REDIS_HOST, REDIS_PORT, app_logger)

    storage = RedisStorage(redis_client)
    state_handler = State(storage)

    pipeline = Pipeline(
        POSTGRES_DSN,
        POSTGRES_SCHEMA,
        DB_QUERY_CHUNK_SIZE,
        ELASTIC_DSN,
        ELASTIC_INDEX_NAME,
        app_logger,
        state_handler
    )

    last_updated = state_handler.get_state("updated")

    start_date = last_updated if last_updated else EPOCH_START_DATE

    while True:
        app_logger.debug("Starting ETL process")
        try:
            pipeline_started_at = datetime.datetime.utcnow()
            state_handler.set_state("updated", pipeline_started_at.isoformat())
            pipeline.do_etl_pipeline(start_date=start_date)
            if start_date == EPOCH_START_DATE:
                subprocess.run(["newman", "run", POSTMAN_TESTS_PATH])
            time.sleep(10)
            start_date = pipeline_started_at
        except (SystemExit, KeyboardInterrupt) as e:
            app_logger.debug("ETL Process stopped")
