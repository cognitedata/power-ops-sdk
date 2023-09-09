from __future__ import annotations

from abc import ABC
from typing import TypeVar, Union

from cognite.client.data_classes import Asset, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes.data_modeling import ContainerId, DataModelId, ViewId
from pydantic import BaseModel
from typing_extensions import TypeAlias

from .cdf_resources import CDFFile, CDFSequence, SpaceId
from .helpers import isinstance_list

_T_Type = TypeVar("_T_Type")


class ResourceType(BaseModel, ABC):
    def sequences(self) -> list[CDFSequence]:
        return self._fields_of_type(CDFSequence)

    def files(self) -> list[CDFFile]:
        return self._fields_of_type(CDFFile)

    def time_series(self) -> list[TimeSeries]:
        return self._fields_of_type(TimeSeries)

    def _fields_of_type(self, type_: type[_T_Type]) -> list[_T_Type]:
        output: list[_T_Type] = []
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if not value:
                continue
            elif isinstance_list(value, type_):
                output.extend(value)
            elif isinstance(value, type_):
                output.append(value)
        return output

    @property
    def external_id(self) -> str:
        raise NotImplementedError()

    def standardize(self) -> None:
        """
        This ensures that the model is in a standardized form.

        This is useful when doing comparisons between models, as for example, the ordering of the assets in some
        lists does not matter.
        """
        ...


Resource: TypeAlias = Union[
    Asset, TimeSeries, Sequence, FileMetadata, ResourceType, ContainerId, ViewId, DataModelId, SpaceId
]
