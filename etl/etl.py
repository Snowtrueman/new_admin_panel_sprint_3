import logging
import time

from extractor import Extractor
from settings import POSTGRES_DSN, POSTGRES_SCHEMA, DB_QUERY_CHUNK_SIZE, EPOCH_START_DATE


if __name__ == "__main__":
    logger_format = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logger_format)
    app_logger = logging.getLogger()
    extractor = Extractor(POSTGRES_DSN, POSTGRES_SCHEMA, DB_QUERY_CHUNK_SIZE, app_logger)

    while True:
        app_logger.debug("Starting process")
        try:
            extractor.extract_data(start_date=EPOCH_START_DATE)
            time.sleep(30)
        except (SystemExit, KeyboardInterrupt):
            app_logger.debug("Process stopped")
