from __future__ import annotations

import abc
from abc import ABC
from collections.abc import Iterable
from typing import Any, Callable, ClassVar, Literal, TypeVar

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataUpdate, SequenceUpdate, TimeSeries, TimeSeriesList
from pydantic import BaseModel
from typing_extensions import Self

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base.resource_type import ResourceType, _T_Type
from cognite.powerops.utils.serialization import remove_read_only_fields

from .cdf_resources import CDFFile, CDFResource, CDFSequence
from .helpers import isinstance_list


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

    def _fields_of_type(self, type_: type[_T_Type]) -> Iterable[_T_Type]:
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if isinstance(value, type_):
                yield value
            elif isinstance_list(value, type_):
                yield from value

    @property
    def model_name(self) -> str:
        return type(self).__name__

    @classmethod
    def name(cls) -> str:
        return cls.__name__

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
    def load_from_cdf_resources(
        cls: type[Self], data: dict[str, Any], link: Literal["external_id", "object"] = "object"
    ) -> Self:
        """
        This function loads the model from the data retrieved from CDF.

        Args:
            data: The data retrieved from CDF.
            link: (Only data models) Whether to link the data model using external IDs or objects. Linking
                objects directly can lead to circular dependencies, thus if this is a concern you should link
                by "external_id".

        Returns:
            Instance of the model.
        """
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
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        ...

    @abc.abstractmethod
    def standardize(self) -> None:
        """
        This ensures that the model is in a standardized form.

        This is useful when doing comparisons between models; as for example, the ordering of the assets in some
        lists does not matter.
        """
        raise NotImplementedError()

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
