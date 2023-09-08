from __future__ import annotations

import abc
from abc import ABC
from typing import ClassVar, Callable, Iterable, Type as TypingType, Any, TypeVar, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes import TimeSeriesList, SequenceUpdate, FileMetadataUpdate
from typing_extensions import Self

from cognite.client.data_classes import TimeSeries
from pydantic import BaseModel
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.diff import Change, FieldDifference, ModelDifference
from cognite.powerops.resync.models.base.resource_type import _T_Type, ResourceType
from .cdf_resources import CDFSequence, CDFFile, CDFResource
from .helpers import isinstance_list
from cognite.powerops.utils.serialization import remove_read_only_fields

from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1


class Model(BaseModel, ABC):
    def sequences(self) -> list[CDFSequence]:
        sequences = [sequence for item in self._resource_types() for sequence in item.sequences()]
        sequences.extend(self._fields_of_type(CDFSequence))
        return sequences

    def files(self) -> list[CDFFile]:
        files = [file for item in self._resource_types() for file in item.files()]
        files.extend(self._fields_of_type(CDFFile))
        return files

    def timeseries(self) -> TimeSeriesList:
        time_series = [ts for item in self._resource_types() for ts in item.time_series()]
        time_series.extend(self._fields_of_type(TimeSeries))
        return TimeSeriesList(time_series)

    cdf_resources: ClassVar[dict[Callable, type]] = {sequences: CDFSequence, files: CDFFile, timeseries: TimeSeries}

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

                def dump(resource: Any) -> dict[str, Any]:
                    return remove_read_only_fields(resource.dump(camel_case=True))

                output[name] = [dump(item) for item in items]
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
    def from_cdf(cls: TypingType[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        ...

    @abc.abstractmethod
    def standardize(self) -> None:
        """
        This ensures that the model is in a standardized form.

        This is useful when doing comparisons between models; as for example, the ordering of the assets in some
        lists does not matter.
        """
        raise NotImplementedError()

    def difference(self, new_model: T_Model) -> ModelDifference:
        if type(self) != type(new_model):
            raise ValueError(f"Cannot compare model of type {type(self)} with {type(new_model)}")
        # The dump and load calls are to remove all read only fields
        current_reloaded = self.load_from_cdf_resources(self.dump_as_cdf_resource())
        new_reloaded = new_model.load_from_cdf_resources(new_model.dump_as_cdf_resource())

        diffs = []
        for field_name in self.model_fields:
            current_value = getattr(current_reloaded, field_name)
            new_value = getattr(new_reloaded, field_name)
            if type(current_value) != type(new_value):
                raise ValueError(
                    f"Cannot compare field {field_name} of type {type(current_value)} with {type(new_value)}"
                )
            diff = self._find_diffs(current_value, new_value, "Domain", field_name)
            diffs.append(diff)

        for function in self.cdf_resources:
            current_items = function(current_reloaded)
            new_items = function(new_reloaded)

            diff = self._find_diffs(current_items, new_items, "CDF", function.__name__)

            diffs.append(diff)

        return ModelDifference(name=self.model_name, changes={d.name: d for d in diffs})

    @staticmethod
    def _find_diffs(
        current_value: Any, new_value: Any, group: Literal["CDF", "Domain"], field_name: str
    ) -> FieldDifference:
        if not current_value and not new_value:
            return FieldDifference(group=group, name=field_name)

        if (
            isinstance(current_value, (list, CogniteResourceList))
            and current_value
            and isinstance(current_value[0], (ResourceType, CogniteResource))
        ) or (
            isinstance(new_value, (list, CogniteResourceList))
            and new_value
            and isinstance(new_value[0], (ResourceType, CogniteResource))
        ):
            current_value_by_id = {item.external_id: item for item in current_value}
            new_value_by_id = {item.external_id: item for item in new_value}
        elif (
            isinstance(current_value, list) and current_value and isinstance(current_value[0], (CDFSequence, CDFFile))
        ) or (isinstance(new_value, list) and new_value and isinstance(new_value[0], (CDFSequence, CDFFile))):
            current_value_by_id = {item.external_id: item.cdf_resource for item in current_value}
            new_value_by_id = {item.external_id: item.cdf_resource for item in new_value}
        elif (
            isinstance(current_value, dict)
            and current_value
            and isinstance(next(iter(current_value.values())), (DomainModelApply, DomainModelApplyCogShop1))
        ) or (
            isinstance(new_value, dict)
            and new_value
            and isinstance(next(iter(new_value.values())), (DomainModelApply, DomainModelApplyCogShop1))
        ):
            current_value_by_id = {item.external_id: item for item in current_value.values()}
            new_value_by_id = {item.external_id: item for item in new_value.values()}
        else:
            raise NotImplementedError(f"{type(current_value)} is not supported")

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
            group=group, name=field_name, added=added, removed=removed, changed=changed, unchanged=unchanged
        )

    @classmethod
    def _add_missing_hash(cls, client: CogniteClient, resource: list[CDFResource]):
        """
        This function adds the hash to the metadata of the resource if it is missing.
        The intention is for backwards compatibility with resources that were created before the hash was added.
        """
        for item in resource:
            resource = item.cdf_resource
            if not resource.metadata:
                resource.metadata = {}
            if CDFResource.content_key_hash not in resource.metadata:
                if isinstance(item, CDFSequence):
                    content = client.sequences.data.retrieve_dataframe(
                        start=0, end=-1, external_id=item.external_id, limit=None
                    )
                    resource.metadata[CDFResource.content_key_hash] = CDFSequence.calculate_hash(content)
                    update = SequenceUpdate(external_id=item.external_id).metadata.add(
                        {CDFSequence.content_key_hash: resource.metadata[CDFResource.content_key_hash]}
                    )
                    client.sequences.update(update)
                elif isinstance(item, CDFFile):
                    content = client.files.download_bytes(item.external_id)
                    resource.metadata[CDFResource.content_key_hash] = CDFFile.calculate_hash(content)
                    update = FileMetadataUpdate(external_id=item.external_id).metadata.add(
                        {CDFFile.content_key_hash: resource.metadata[CDFResource.content_key_hash]}
                    )
                    client.files.update(update)
                else:
                    raise NotImplementedError(f"Cannot get content for {item}")


T_Model = TypeVar("T_Model", bound=Model)
