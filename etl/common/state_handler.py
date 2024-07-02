import abc
import json
from typing import Any, Dict

from clients.redis_client import RedisClient
from settings import REDIS_BASIC_STORAGE_KEY


class BaseStorage(abc.ABC):
    """
    Abstract state handler
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """
        Saves state in storage
        """

        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """
        Receives state from storage
        """

        raise NotImplementedError


class RedisStorage(BaseStorage):
    """
    Class for saving state in Redis storage
    """

    def __init__(self, redis_client: RedisClient):
        self.__redis_client: RedisClient = redis_client

    def save_state(self, state: Dict[str, Any]) -> None:
        """
        Saves state in storage
        """

        self.__redis_client.set(REDIS_BASIC_STORAGE_KEY, json.dumps(state))

    def retrieve_state(self) -> Dict[str, Any]:
        """
        Receives state from storage
        """

        state = self.__redis_client.get(REDIS_BASIC_STORAGE_KEY)
        if state:
            return json.loads(state)
        else:
            return {}


class State:
    """
    Class for handling the system state
    """

    def __init__(self, storage: BaseStorage) -> None:
        self.__storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """
        Set state for provided key
        """

        actual_state_dict = self.__storage.retrieve_state()
        actual_state_dict.update({key: value})
        self.__storage.save_state(actual_state_dict)

    def get_state(self, key: str) -> Any:
        """
        Receives state via provided state
        """

        actual_state_dict = self.__storage.retrieve_state()
        return actual_state_dict.get(key)
