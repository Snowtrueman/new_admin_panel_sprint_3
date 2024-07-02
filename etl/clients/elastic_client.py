import os
import json
from json import JSONDecodeError
import logging
from typing import Optional, Any

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch, TransportError, ApiError
from elasticsearch.helpers import bulk

from common.modifiers import Singleton, backoff
from common.serializers import ElasticFilmWorkSerializer


class ElasticClient(metaclass=Singleton):
    """
    Client class for interacting with Elasticsearch
    """

    def __init__(self, elastic_dsn: str, elastic_index_name: str, logger: logging.Logger):
        self.__elastic_dsn: str = elastic_dsn
        self.__elastic_index_name: str = elastic_index_name
        self.__client: Optional[Elasticsearch] = None
        self.__logger: logging.Logger = logger
        self.__execution_valid_methods: Optional[list] = None

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def connection(self) -> Elasticsearch:
        """
        Returns connection to Elastic
        """

        if not hasattr(self, "__client"):
            self.__client = self.__get_elastic_connection()
        return self.__client

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

        if not self.__execution_valid_methods:
            self.__execution_valid_methods = self.__get_valid_methods()

        if command not in self.__execution_valid_methods:
            self.__logger.error(f"Failed to execute unrecognised command {command}")
            return
        try:

            return getattr(self, f"_{command}")(self.connection, *args, **kwargs)
        except TransportError as exception:
            self.__logger.error(f"An error while trying to establish connection to "
                                f"Elasticsearch using DSN: {self.__elastic_dsn}")
            raise exception

    @backoff()
    def __get_elastic_connection(self) -> Optional[Elasticsearch]:
        """
        Tries to establish connection to Elastic using provided dsn
        """

        try:
            client = Elasticsearch(self.__elastic_dsn)
            if self.__test_connection(client):
                return client
        except TransportError as exception:
            self.__logger.error(f"An error while trying to establish connection to "
                                f"Elasticsearch using DSN: {self.__elastic_dsn}")
            raise exception

    @staticmethod
    def __test_connection(elastic_client: Elasticsearch) -> ObjectApiResponse:
        """
        Checks if connection is alive
        """

        return elastic_client.info()

    def __load_index_schema_mapping(self, path_to_schema: str) -> Optional[dict]:
        """
        Loads index schema mapping from provided json file
        """

        if not os.path.isfile(path_to_schema):
            self.__logger.error(f"Can't create index {self.__elastic_index_name} because the schema file is invalid")
            return

        with open(path_to_schema, "r") as schema:
            try:
                mapping = json.loads(schema.read())
            except JSONDecodeError:
                self.__logger.error(f"Can't create index {self.__elastic_index_name} "
                                    f"because of wrong index schema format")
                return
            else:
                return mapping

    def _create_index(self, elastic_client: Elasticsearch, path_to_schema: str) -> Optional[ObjectApiResponse]:
        """
        Creates index according to provided schema
        """

        mapping = self.__load_index_schema_mapping(path_to_schema)

        if not mapping:
            return

        if not self.__elastic_index_name:
            self.__logger.error("Can't create index - index name is not provided ")
            return

        if not elastic_client.indices.exists(index=self.__elastic_index_name):
            try:
                created = elastic_client.indices.create(index=self.__elastic_index_name, body=mapping)
                if created:
                    self.__logger.debug(f"Successfully created index {self.__elastic_index_name}")
                    return created
            except ApiError as exception:
                self.__logger.error("Can't create index - an error in index name of other ES exception")
                raise exception
            except TransportError as exception:
                self.__logger.error(f"An error while trying to create index - "
                                    f"Elasticsearch connection error using DSN: {self.__elastic_dsn}")
                raise exception

    def _delete_index(self, elastic_client: Elasticsearch, index_name: str) -> Optional[ObjectApiResponse]:
        """
        Removes index by its name
        """

        if not index_name:
            self.__logger.error("Can't delete index - index name is not provided ")
            return
        try:
            deleted = elastic_client.indices.delete(index=index_name)
            return deleted
        except ApiError as exception:
            self.__logger.error("Can't delete index - no such index or other ES exception")
            raise exception
        except TransportError as exception:
            self.__logger.error(f"An error while trying to delete index - "
                                f"Elasticsearch connection error using DSN: {self.__elastic_dsn}")
            raise exception

    def _load_data(self, elastic_client: Elasticsearch, data: list[ElasticFilmWorkSerializer]) -> tuple:
        """
        Loads provided data records to Elasticsearch
        """

        documents = [{"_index": self.__elastic_index_name, "_id": row.id, "_source": row.json()} for row in data]
        try:
            loaded_data = bulk(elastic_client, documents)
            self.__logger.debug(f"Successfully uploaded to Elasticsearch {loaded_data[0]} film work objects ")
            return loaded_data
        except ApiError as exception:
            self.__logger.error("An error in data uploading")
            raise exception
        except TransportError as exception:
            self.__logger.error(f"An error while trying to upload data to index - "
                                f"Elasticsearch connection error using DSN: {self.__elastic_dsn}")
            raise exception
