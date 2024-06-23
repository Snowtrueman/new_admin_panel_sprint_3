import logging

from extractor import Extractor
from settings import POSTGRES_DSN, DB_QUERY_CHUNK_SIZE


if __name__ == "__main__":
    logger_format = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logger_format)
    app_logger = logging.getLogger()
    extractor = Extractor(POSTGRES_DSN, DB_QUERY_CHUNK_SIZE, app_logger)

    extractor.do_persons_pipeline()
