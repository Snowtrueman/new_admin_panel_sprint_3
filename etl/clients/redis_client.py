import logging
from typing import Optional, Any

from redis import Redis, ConnectionPool
from redis.commands.core import ResponseT

from common.modifiers import Singleton, backoff


class RedisClient(metaclass=Singleton):
    """
    Redis client
    """

    def __init__(self, host: str, port: int, logger: logging.Logger):
        self.__host: str = host
        self.__port: int = port
        self.__logger: logging.Logger = logger
        self.__connection: Optional[Redis] = None

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def connection(self) -> Redis:
        """
        Returns connection to Redis
        """

        if not hasattr(self, "__connection"):
            self.__get_connection()
        return self.__connection

    @backoff()
    def __get_connection(self) -> None:
        """
        Tries to establish Redis connection
        """

        pool = ConnectionPool(host=self.__host, port=self.__port)
        self.__connection = Redis(connection_pool=pool)

    @backoff()
    def get(self, key: str) -> ResponseT:
        """
        Gets the value from Redis by its key
        """

        return self.connection.get(key)

    @backoff()
    def set(self, key: str, value: Any) -> ResponseT:
        """
        Set the provided value for the provided key
        """

        return self.connection.set(key, value)
