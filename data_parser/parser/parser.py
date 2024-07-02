import logging
import sys
from typing import Optional, Type, Any

from settings import EXPORT_TABLE_DATACLASS_MAPPER, IMPORT_TABLE_FIELDS_MAPPER
import serializers


class DataParser:
    """
    Serializes income data into appropriate dataclass object
    """

    def __init__(self, table_name: str, logger: logging.Logger):
        self._table_name: str = table_name
        self._logger: logging.Logger = logger
        self._dataclass: Optional[Type[Any]] = None
        self._data_objects_list: Optional[list[Type[Any], ...]] = None

        self._get_target_dataclass()

    def _get_target_dataclass(self) -> None:
        """
        Get appropriate data class by export table name
        """

        dataclass_name = EXPORT_TABLE_DATACLASS_MAPPER.get(self._table_name)
        if not dataclass_name:
            self._logger.error(f"Error while trying to parse data from table {self._table_name} - no appropriate"
                               f"data class was found")
            self._dataclass = None
            return
        try:
            dataclass = getattr(sys.modules['serializers'], dataclass_name)
            self._dataclass = dataclass
        except AttributeError:
            self._logger.error(f"Error while trying to parse data from table {self._table_name} - no appropriate"
                               f"data class was found")
            self._dataclass = None

    def parse_data(self, income_data: list[dict, ...], ) -> Optional[list]:
        """
        Perform data parsing
        """

        if self._dataclass:
            self._data_objects_list = []
            for data_item in income_data:
                mapped_item = dict({value: data_item[key]
                                    for key, value in IMPORT_TABLE_FIELDS_MAPPER.get(self._table_name).items()})
                self._data_objects_list.append(self._dataclass(**mapped_item))
            return self._data_objects_list
