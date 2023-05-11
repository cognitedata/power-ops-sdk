from __future__ import annotations

import json
from collections import UserList
from typing import Any, Collection, Optional, Type, TypeVar

import pandas as pd
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._pandas_helpers import convert_nullable_int_cols, notebook_display_with_fallback
from cognite.client.utils._time import convert_time_attributes_to_datetime
from pydantic import BaseModel

T_AssetResource = TypeVar("T_AssetResource", bound=BaseModel)


class AssetResourceList(UserList):
    _RESOURCE: Type[BaseModel]

    def __init__(self, resources: Collection[Any]):
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{self.__class__.__name__}' must be of type "
                    f"'{self._RESOURCE.__name__}', not '{type(resource)}'."
                )
        super().__init__(resources)
        self._id_to_item, self._external_id_to_item = {}, {}
        if self.data:
            if hasattr(self.data[0], "external_id"):
                self._external_id_to_item = {
                    item.external_id: item for item in self.data if item.external_id is not None
                }
            if hasattr(self.data[0], "id"):
                self._id_to_item = {item.id: item for item in self.data if item.id is not None}

    def __getitem__(self, item: Any) -> Any:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return self.__class__(value)
        return value

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, indent=4)

    def extend(self, other: Collection[Any]) -> None:
        other_res_list = type(self)(other)  # See if we can accept the types
        if set(self._id_to_item).isdisjoint(other_res_list._id_to_item):
            super().extend(other)
            self._external_id_to_item.update(other_res_list._external_id_to_item)
            self._id_to_item.update(other_res_list._id_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def dump(self) -> list[dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dict(exclude_unset=True) for resource in self.data]

    def get(self, id: int = None, external_id: str = None) -> Optional[T_AssetResource]:
        """Get an item from this list by id or exernal_id.

        Args:
            id (int): The id of the item to get.
            external_id (str): The external_id of the item to get.

        Returns:
            Optional[CogniteResource]: The requested item
        """
        IdentifierSequence.load(id, external_id).assert_singleton()
        if id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    def to_pandas(self, camel_case: bool = False) -> pd.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        df = pd.DataFrame(self.dump())
        return convert_nullable_int_cols(df, camel_case)

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)


T_AssetResourceList = TypeVar("T_AssetResourceList", bound=AssetResourceList)
