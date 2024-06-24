import os
import abc
import json
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.file_path = file_path


    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

        with open(self.file_path, "w") as storage:
            storage.write(json.dumps(state))

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""

        if os.path.isfile(self.file_path):
            with open(self.file_path) as storage:
                state = storage.read()
                return json.loads(state)
        else:
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""

        actual_state_dict = self.storage.retrieve_state()
        actual_state_dict.update({key: value})
        self.storage.save_state(actual_state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""

        actual_state_dict = self.storage.retrieve_state()
        return actual_state_dict.get(key)



# from redis import Redis
#
#
# class RedisStorage(BaseStorage):
#   def __init__(self, redis_adapter: Redis):
#     self.redis_adapter = redis_adapter

STATE_TEMPLATE = {
    "postgres": {
        "genres": {
            "status": "",
            "date": "",
            "offset": ""
        },
        "person": {
            "status": "",
            "date": "",
            "offset": ""
        },
        "film_work": {
            "status": "",
            "date": "",
            "offset": ""
        }
    },
    "elastic": {
        "status": ""
    },
    "date": ""
}