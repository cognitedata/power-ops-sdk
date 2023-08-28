from __future__ import annotations

import abc
from abc import ABC
from typing import ClassVar, Callable, Iterable, Type as TypingType, Any, TypeVar
from typing_extensions import Self

from cognite.client.data_classes import TimeSeries
from pydantic import BaseModel

from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.cdf_resources import CDFSequence, CDFFile
from cognite.powerops.resync.models.helpers import isinstance_list
from cognite.powerops.resync.utils.serializer import remove_read_only_fields

_T_Type = TypeVar("_T_Type")


class ResourceType(BaseModel, ABC):
    def sequences(self) -> list[CDFSequence]:
        return self._fields_of_type(CDFSequence)

    def files(self) -> list[CDFFile]:
        return self._fields_of_type(CDFFile)

    def time_series(self) -> list[TimeSeries]:
        return self._fields_of_type(TimeSeries)

    def _fields_of_type(self, type_: TypingType[_T_Type]) -> list[_T_Type]:
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


class Model(BaseModel, ABC):
    def sequences(self) -> list[CDFSequence]:
        sequences = [sequence for item in self._resource_types() for sequence in item.sequences()]
        sequences.extend(self._fields_of_type(CDFSequence))
        return sequences

    def files(self) -> list[CDFFile]:
        files = [file for item in self._resource_types() for file in item.files()]
        files.extend(self._fields_of_type(CDFFile))
        return files

    def timeseries(self) -> list[TimeSeries]:
        time_series = [ts for item in self._resource_types() for ts in item.time_series()]
        time_series.extend(self._fields_of_type(TimeSeries))
        return time_series

    cdf_resources: ClassVar[dict[Callable, type]] = {
        sequences: CDFSequence,
        files: CDFFile,
        timeseries: TimeSeries,
    }

    def _resource_types(self) -> Iterable[ResourceType]:
        yield from self._fields_of_type(ResourceType)

    def _fields_of_type(self, type_: TypingType[_T_Type]) -> Iterable[_T_Type]:
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if isinstance(value, type_):
                yield value
            elif isinstance_list(value, type_):
                yield from value

    @property
    def model_name(self) -> str:
        return type(self).__name__

    def dump_as_cdf_resource(self) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for resource_fun in self.cdf_resources.keys():
            name = resource_fun.__name__
            if items := resource_fun(self):
                if name == "sequences":
                    # Sequences must clean columns as well.
                    def dump(resource: CDFSequence) -> dict[str, Any]:
                        output = remove_read_only_fields(resource.dump(camel_case=True))
                        if (columns := output.get("columns")) and isinstance(columns, list):
                            for no, column in enumerate(columns):
                                output["columns"][no] = remove_read_only_fields(column)
                        return output

                else:

                    def dump(resource: Any) -> dict[str, Any]:
                        return remove_read_only_fields(resource.dump(camel_case=True))

                output[name] = sorted((dump(item) for item in items), key=self._external_id_key)
        return output

    @classmethod
    def _load_by_type_external_id(cls, data: dict[str, Any]) -> dict[str, dict[str, Any]]:
        loaded_by_type_external_id: dict[str, dict[str, Any]] = {}
        for function, resource_cls in cls.cdf_resources.items():
            name = function.__name__
            if items := data.get(name):
                loaded_by_type_external_id[name] = {
                    loaded.external_id: loaded
                    for loaded in (resource_cls._load(item) if isinstance(item, dict) else item for item in items)
                }
        return loaded_by_type_external_id

    @classmethod
    @abc.abstractmethod
    def load_from_cdf_resources(cls: TypingType[Self], data: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _external_id_key(cls, resource: dict | Any) -> str:
        if hasattr(resource, "external_id"):
            return resource.external_id.casefold()
        elif isinstance(resource, dict) and ("external_id" in resource or "externalId" in resource):
            return resource.get("external_id", resource.get("externalId")).casefold()
        raise ValueError(f"Could not find external_id in {resource}")

    @classmethod
    @abc.abstractmethod
    def from_cdf(
        cls: TypingType[T_Model],
        client: PowerOpsClient,
        data_set_external_id: str,
    ) -> T_Model:
        ...


T_Model = TypeVar("T_Model", bound=Model)
