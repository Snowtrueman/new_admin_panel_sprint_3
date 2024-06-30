import datetime
import logging
from typing import Optional

from settings import EPOCH_START_DATE
from extractor import Extractor
from transformer import FilmWorkMerger, PostgresToElasticTransformer
from loader import ElasticLoader
from postgres_client import PostgresClient
from elastic_client import ElasticClient
from constants import Entities


class Pipeline:
    """
    Class for performing ETL pipeline
    """

    def __init__(self,
                 postgres_dns: dict,
                 postgres_schema: str,
                 chunk_size: int,
                 elastic_dsn: str,
                 elastic_index_name: str,
                 logger: logging.Logger
                 ):

        self._logger: logging.Logger = logger


        self._extractor: Extractor = self._get_extractor(postgres_dns, postgres_schema, chunk_size)
        self._merger: FilmWorkMerger = self._get_merger()
        self._transformer: PostgresToElasticTransformer = self._get_transformer()
        self._loader: ElasticLoader = self._get_loader(elastic_dsn, elastic_index_name)

    def _get_extractor(self, postgres_dns: dict, postgres_schema: str, chunk_size: int):
        """
        Gets the instance of extractor class
        """

        postgres_client = PostgresClient(postgres_dns, postgres_schema, chunk_size, self._logger)
        return Extractor(postgres_client, postgres_schema, chunk_size, self._logger)

    def _get_merger(self):
        """
        Gets the instance of merger class
        """
        return FilmWorkMerger(self._logger)

    def _get_transformer(self):
        """
        Gets the instance of transformer class
        """

        return PostgresToElasticTransformer(self._logger)

    def _get_loader(self, elastic_dsn: str, elastic_index_name: str):
        """
        Gets the instance if loader class
        """

        elastic_client = ElasticClient(elastic_dsn, elastic_index_name, self._logger)
        return ElasticLoader(elastic_client, self._logger)

    def _do_basic_pipeline(self, start_date: datetime.datetime, target_film_works: Optional[list] = None):
        """
        Performs database check based on film work records
        """

        extracted_film_works = self._extractor.extract_changed_film_works(start_date, target_film_works)
        if extracted_film_works:
            merged_film_works = self._merger.merge_persons(extracted_film_works)
            if merged_film_works:
                transformed_film_works = self._transformer.transform(merged_film_works)
                if transformed_film_works:
                    self._loader.load_data(transformed_film_works)

    def _do_full_pipeline(self, start_date: datetime.datetime):
        """
        Performs full database check based on film work records and possible changes in related entities
        """

        for entity in [e.value for e in Entities]:
            entity_changes = self._extractor.find_changes_in_related_entities(entity, start_date)
            self._do_basic_pipeline(start_date, entity_changes)

        self._do_basic_pipeline(start_date)

    def do_etl_pipeline(self, start_date: datetime.datetime):
        """
        Performs complex database check
        """

        if start_date == EPOCH_START_DATE:
            self._do_basic_pipeline(start_date)
        else:
            self._do_full_pipeline(start_date)
