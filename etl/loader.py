from typing import Optional
import logging

from elastic_transport import ObjectApiResponse

from serializers import ElasticFilmWorkSerializer
from elastic_client import ElasticClient


class ElasticLoader:
    """
    Class for uploading entities to Elasticsearch
    """

    def __init__(self, elastic_client: ElasticClient, logger: logging.Logger):
        self._elastic_client: ElasticClient = elastic_client
        self._logger: logging.Logger = logger

    def create_index(self, path_to_schema: str) -> Optional[ObjectApiResponse]:
        """
        Creates Elasticsearch index according to provided schema
        """

        created = self._elastic_client.execute(command="create_index", path_to_schema=path_to_schema)
        return created

    def delete_index(self, index_name: str) -> Optional[ObjectApiResponse]:
        """
        Removes Elasticsearch index according to provided index name
        """

        deleted = self._elastic_client.execute(command="delete_index", index_name=index_name)
        return deleted

    def load_data(self, transformed_film_works: list[ElasticFilmWorkSerializer]) -> Optional[tuple]:
        """
        Loads provided data records to Elasticsearch
        """

        connected = self._elastic_client.execute("connect")
        if connected:
            loaded_data = self._elastic_client.execute(command="load_data", data=transformed_film_works)
            return loaded_data
