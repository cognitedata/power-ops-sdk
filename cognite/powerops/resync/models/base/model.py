from __future__ import annotations

import abc
from abc import ABC
from dataclasses import dataclass, field
from typing import ClassVar, Callable, Iterable, Type as TypingType, Any, TypeVar, Literal
from typing_extensions import Self

from cognite.client.data_classes import TimeSeries
from pydantic import BaseModel

from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.cdf_resources import CDFSequence, CDFFile
from cognite.powerops.resync.models.helpers import isinstance_list
from cognite.powerops.resync.utils.serializer import remove_read_only_fields

_T_Type = TypeVar("_T_Type")


@dataclass
class Change:
    last: dict[str, Any]
    new: dict[str, Any]


@dataclass
class FieldSummary:
    group: Literal["CDF", "Domain"]
    name: str
    added: int
    removed: int
    changed: int
    unchanged: int


@dataclass
class FieldDifference:
    group: Literal["CDF", "Domain"]
    name: str
    added: list[dict[str, Any]] = field(default_factory=list)
    removed: list[dict[str, Any]] = field(default_factory=list)
    changed: list[Change] = field(default_factory=list)
    unchanged: list[dict[str, Any]] = field(default_factory=list)

    def as_summary(self) -> FieldSummary:
        return FieldSummary(
            group=self.group,
            name=self.name,
            added=len(self.added),
            removed=len(self.removed),
            changed=len(self.changed),
            unchanged=len(self.unchanged),
        )


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

    @abc.abstractmethod
    def sort_lists(self) -> None:
        """
        This is used to standardize the order of lists in the model, which is useful for comparing models.
        """
        raise NotImplementedError()

    def dump_as_cdf_resource(self) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for resource_fun in self.cdf_resources.keys():
            name = resource_fun.__name__
            if items := resource_fun(self):

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

    def difference(self, new_model: T_Model) -> list[FieldDifference]:
        if type(self) != type(new_model):
            raise ValueError(f"Cannot compare model of type {type(self)} with {type(new_model)}")
        self.sort_lists()
        new_model.sort_lists()
        diffs = []
        for field_name in self.model_fields:
            current_value = getattr(self, field_name)
            new_value = getattr(new_model, field_name)
            if type(current_value) != type(new_value):
                raise ValueError(
                    f"Cannot compare field {field_name} of type {type(current_value)} with {type(new_value)}"
                )
            diff = self._find_diffs(current_value, new_value, "Domain", field_name)
            diffs.append(diff)

        current_cdf_resources = self.dump_as_cdf_resource()
        new_cdf_resources = new_model.dump_as_cdf_resource()
        for field_name in set(current_cdf_resources.keys()) | set(new_cdf_resources.keys()):
            if field_name in current_cdf_resources and field_name in new_cdf_resources:
                diff = self._find_diffs(
                    current_cdf_resources[field_name], new_cdf_resources[field_name], "CDF", field_name
                )
            elif field_name in current_cdf_resources:
                diff = FieldDifference(group="CDF", name=field_name, removed=current_cdf_resources[field_name])
            elif field_name in new_cdf_resources:
                diff = FieldDifference(group="CDF", name=field_name, added=new_cdf_resources[field_name])
            else:
                raise ValueError(f"Field {field_name} is not in current or new model")
            diffs.append(diff)

        return diffs

    @staticmethod
    def _find_diffs(
        current_value: Any, new_value: Any, group: Literal["CDF", "Domain"], field_name: str
    ) -> FieldDifference:
        if (
            isinstance(current_value, list)
            and current_value
            and isinstance(current_value[0], (ResourceType, CDFFile, CDFSequence))
        ):
            current_value_by_id = {item.external_id: item.model_dump() for item in current_value}
            new_value_by_id = {item.external_id: item.model_dump() for item in new_value}
        elif isinstance(current_value, list) and current_value and isinstance(current_value[0], dict):
            current_value_by_id = {item["externalId"]: item for item in current_value}
            new_value_by_id = {item["externalId"]: item for item in new_value}
        elif (
            isinstance(current_value, dict)
            and current_value
            and hasattr(next(iter(current_value.values())), "model_dump")
        ):
            current_value_by_id = {item.external_id: item.model_dump() for item in current_value.values()}
            new_value_by_id = {item.external_id: item.model_dump() for item in new_value.values()}
        else:
            raise NotImplementedError(f"Only list of resources are supported, {type(current_value)} is not supported")

        added_ids = set(new_value_by_id.keys()) - set(current_value_by_id.keys())
        added = [new_value_by_id[external_id] for external_id in sorted(added_ids)]

        removed_ids = set(current_value_by_id.keys()) - set(new_value_by_id.keys())
        removed = [current_value_by_id[external_id] for external_id in sorted(removed_ids)]

        existing_ids = set(current_value_by_id.keys()) & set(new_value_by_id.keys())
        changed = []
        unchanged = []
        for existing in sorted(existing_ids):
            current = current_value_by_id[existing]
            new = new_value_by_id[existing]
            if current == new:
                unchanged.append(current)
            else:
                changed.append(Change(last=current, new=new))

        return FieldDifference(
            group=group,
            name=field_name,
            added=added,
            removed=removed,
            changed=changed,
            unchanged=unchanged,
        )


T_Model = TypeVar("T_Model", bound=Model)
