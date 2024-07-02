from typing import Optional
import logging

from elastic_transport import ObjectApiResponse

from common.serializers import ElasticFilmWorkSerializer
from clients.elastic_client import ElasticClient


class ElasticLoader:
    """
    Class for uploading entities to Elasticsearch
    """

    def __init__(self, elastic_client: ElasticClient, logger: logging.Logger):
        self.__elastic_client: ElasticClient = elastic_client
        self.__logger: logging.Logger = logger

    def create_index(self, path_to_schema: str) -> Optional[ObjectApiResponse]:
        """
        Creates Elasticsearch index according to provided schema
        """

        created = self.__elastic_client.execute(command="create_index", path_to_schema=path_to_schema)
        return created

    def delete_index(self, index_name: str) -> Optional[ObjectApiResponse]:
        """
        Removes Elasticsearch index according to provided index name
        """

        deleted = self.__elastic_client.execute(command="delete_index", index_name=index_name)
        return deleted

    def load_data(self, transformed_film_works: list[ElasticFilmWorkSerializer]) -> Optional[tuple]:
        """
        Loads provided data records to Elasticsearch
        """

        loaded_data = self.__elastic_client.execute(command="load_data", data=transformed_film_works)
        return loaded_data
