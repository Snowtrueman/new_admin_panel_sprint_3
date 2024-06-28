import os
import json
from json import JSONDecodeError
import logging
from typing import Optional, Any

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch, TransportError, ApiError
from elasticsearch.helpers import bulk

from common import backoff
from transformer import ElasticFilmWorkSerializer


class ElasticClient:
    """
    Client class for interacting with Elasticsearch
    """

    def __init__(self, elastic_dsn: str, elastic_index_name: str, logger: logging.Logger):
        self._elastic_dsn: str = elastic_dsn
        self._elastic_index_name: str = elastic_index_name
        self._client: Optional[Elasticsearch] = None
        self._logger: logging.Logger = logger
        self._execution_valid_methods: list = self.__get_valid_methods()

    @property
    def logger(self):
        return self._logger

    def __get_valid_methods(self) -> list:
        """
        Returns the list of available methods for external call
        """

        return [
            item.strip("_") for item in self.__class__.__dict__.keys()
            if not item.startswith("__")
            and not item.startswith(f"_{self.__class__.__name__}")
            and callable(getattr(self, item))
        ]

    @backoff()
    def execute(self, command: str, *args, **kwargs) -> Any:
        """
        Execute provided command
        """

        if command not in self._execution_valid_methods:
            self._logger.error(f"Failed to execute unrecognised command {command}")
            return
        return getattr(self, f"_{command}")(*args, **kwargs)

    def _connect(self) -> Optional[bool]:
        """
        Tries to establish connection to Elastic using provided dsn
        """

        try:
            self._client = Elasticsearch(self._elastic_dsn)
            if self.__test_connection():
                return True
        except TransportError as exception:
            self._logger.error(f"An error while trying to establish connection to "
                               f"Elasticsearch using DSN: {self._elastic_dsn}")
            raise exception

    def __test_connection(self) -> ObjectApiResponse:
        """
        Checks if connection is alive
        """

        return self._client.info()

    def __load_index_schema_mapping(self, path_to_schema: str) -> Optional[dict]:
        """
        Loads index schema mapping from provided json file
        """

        if not os.path.isfile(path_to_schema):
            self._logger.error(f"Can't create index {self._elastic_index_name} because the schema file is invalid")
            return

        with open(path_to_schema, "r") as schema:
            try:
                mapping = json.loads(schema.read())
            except JSONDecodeError:
                self._logger.error(f"Can't create index {self._elastic_index_name} "
                                   f"because of wrong index schema format")
                return
            else:
                return mapping

    def _create_index(self, path_to_schema: str) -> Optional[ObjectApiResponse]:
        """
        Creates index according to provided schema
        """

        mapping = self.__load_index_schema_mapping(path_to_schema)

        if not mapping:
            return

        if not self._elastic_index_name:
            self._logger.error("Can't create index - index name is not provided ")
            return

        if not self._client.indices.exists(index=self._elastic_index_name):
            try:
                created = self._client.indices.create(index=self._elastic_index_name, body=mapping)
                if created:
                    self._logger.debug(f"Successfully created index {self._elastic_index_name}")
                    return created
            except ApiError as exception:
                self._logger.error("Can't create index - an error in index name of other ES exception")
                raise exception
            except TransportError as exception:
                self._logger.error(f"An error while trying to create index - "
                                   f"Elasticsearch connection error using DSN: {self._elastic_dsn}")
                raise exception

    def _delete_index(self, index_name: str) -> Optional[ObjectApiResponse]:
        """
        Removes index by its name
        """

        if not index_name:
            self._logger.error("Can't delete index - index name is not provided ")
            return
        try:
            deleted = self._client.indices.delete(index=index_name)
            return deleted
        except ApiError as exception:
            self._logger.error("Can't delete index - no such index or other ES exception")
            raise exception
        except TransportError as exception:
            self._logger.error(f"An error while trying to delete index - "
                               f"Elasticsearch connection error using DSN: {self._elastic_dsn}")
            raise exception

    def _load_data(self, data: list[ElasticFilmWorkSerializer]) -> tuple:
        """
        Loads provided data records to Elasticsearch
        """

        documents = [{"_index": self._elastic_index_name, "_id": row.id, "_source": row.json()} for row in data]
        try:
            loaded_data = bulk(self._client, documents)
            return loaded_data
        except ApiError as exception:
            self._logger.error("An error in data uploading")
            raise exception
        except TransportError as exception:
            self._logger.error(f"An error while trying to upload data to index - "
                               f"Elasticsearch connection error using DSN: {self._elastic_dsn}")
            raise exception
