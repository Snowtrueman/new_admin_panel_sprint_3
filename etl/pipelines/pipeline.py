import datetime
import logging
from typing import Optional

from settings import EPOCH_START_DATE, ELASTIC_SCHEMA
from pipelines.extractor import Extractor
from .transformer import FilmWorkMerger, PostgresToElasticTransformer
from .loader import ElasticLoader
from clients.postgres_client import PostgresClient
from clients.elastic_client import ElasticClient
from common.constants import Entities
from common.state_handler import State


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
                 logger: logging.Logger,
                 state_handler: State
                 ):

        self.__logger: logging.Logger = logger
        self.__state_handler: State = state_handler

        self.__extractor: Extractor = self.__get_extractor(postgres_dns, postgres_schema, chunk_size)
        self.__merger: FilmWorkMerger = self.__get_merger()
        self.__transformer: PostgresToElasticTransformer = self.__get_transformer()
        self.__elastic_client: Optional[ElasticClient] = None
        self.__loader: ElasticLoader = self.__get_loader(elastic_dsn, elastic_index_name)

    def __get_extractor(self, postgres_dns: dict, postgres_schema: str, chunk_size: int) -> Extractor:
        """
        Gets the instance of extractor class
        """

        postgres_client = PostgresClient(postgres_dns, postgres_schema, chunk_size, self.__logger)
        return Extractor(postgres_client, postgres_schema, chunk_size, self.__logger)

    def __get_merger(self) -> FilmWorkMerger:
        """
        Gets the instance of merger class
        """

        return FilmWorkMerger(self.__logger)

    def __get_transformer(self) -> PostgresToElasticTransformer:
        """
        Gets the instance of transformer class
        """

        return PostgresToElasticTransformer(self.__logger)

    def __get_loader(self, elastic_dsn: str, elastic_index_name: str) -> ElasticLoader:
        """
        Gets the instance if loader class
        """

        elastic_client = ElasticClient(elastic_dsn, elastic_index_name, self.__logger)
        self.__elastic_client = elastic_client
        return ElasticLoader(elastic_client, self.__logger)

    def __do_basic_pipeline(self, start_date: datetime.datetime, target_film_works: Optional[list] = None) -> None:
        """
        Performs database check based on film work records
        """

        if not target_film_works:
            extracted_film_works = self.__extractor.extract_changed_film_works(start_date, target_film_works)
        else:
            extracted_film_works = target_film_works
        if extracted_film_works:
            merged_film_works = self.__merger.merge_persons(extracted_film_works)
            if merged_film_works:
                transformed_film_works = self.__transformer.transform(merged_film_works)
                if transformed_film_works:
                    self.__loader.load_data(transformed_film_works)

    def __do_full_pipeline(self, start_date: datetime.datetime) -> None:
        """
        Performs full database check based on film work records and possible changes in related entities
        """

        for entity in [e.value for e in Entities]:
            entity_changes = self.__extractor.find_changes_in_related_entities(entity, start_date)
            self.__do_basic_pipeline(start_date, entity_changes)

        self.__do_basic_pipeline(start_date)

    def do_etl_pipeline(self, start_date: datetime.datetime) -> None:
        """
        Performs complex database check
        """

        index_created = self.__state_handler.get_state("index_created")

        if not index_created:
            self.__elastic_client.execute(command="create_index", path_to_schema=ELASTIC_SCHEMA)
            self.__state_handler.set_state("index_created", True)

        if start_date == EPOCH_START_DATE:
            self.__do_basic_pipeline(start_date)
        else:
            self.__do_full_pipeline(start_date)
