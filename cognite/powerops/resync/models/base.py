from __future__ import annotations

import abc
from collections import defaultdict, Counter
from itertools import groupby
import json
from abc import ABC

from cognite.client.exceptions import CogniteNotFoundError
from deepdiff import DeepDiff
from deepdiff.model import PrettyOrderedSet

from pathlib import Path
from typing import Any, ClassVar, Iterable, Optional, Union, TypeVar, get_args
from typing import Type as TypingType
from pydantic import BaseModel, ConfigDict

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Label, Relationship, TimeSeries
from cognite.client.data_classes.data_modeling.instances import (
    EdgeApply,
    NodeApply,
    InstancesApply,
    NodeApplyList,
    EdgeApplyList,
)

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.clients.data_classes._core import DomainModelApply
from cognite.powerops.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence
from cognite.powerops.resync.models.helpers import (
    format_change_binary,
    format_value_added,
    format_value_removed,
    isinstance_list,
    match_field_from_relationship,
    pydantic_model_class_candidate,
)
from cognite.powerops.resync.utils.common import all_subclasses
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


class AssetType(ResourceType, ABC):
    parent_external_id: ClassVar[str]
    label: ClassVar[Union[AssetLabel, str]]
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)
    parent_description: ClassVar[Optional[str]] = None
    _instantiated_assets: ClassVar[dict[str, AssetType]] = defaultdict(dict)

    name: str
    description: Optional[str] = None
    _external_id: Optional[str] = None
    _type: Optional[str] = None

    @property
    def external_id(self) -> str:
        return self._external_id or f"{self.type_}_{self.name}"

    @external_id.setter
    def external_id(self, value: str) -> None:
        """
        This setter is only used in the Market models which have inconsistent naming between the type and the
        parent_external_id. It is a workaround to keep the refactoring to models to avoid introducing breaking changes
        in CDF.
        Parameters
        ----------
        value : str
            The external id of the asset.

        """
        self._external_id = value

    @property
    def type_(self) -> str:
        return self._type or self.parent_external_id.removesuffix("s")

    @type_.setter
    def type_(self, value: str) -> None:
        """
        This setter is only used in the Market models which have inconsistent naming between the type and
        parent_external_id. It is a workaround to keep the refactoring to models to avoid introducing breaking changes
        in CDF.

        Parameters
        ----------
        value: str
            The type of the asset.

        """
        self._type = value

    @property
    def parent_name(self):
        return self.parent_external_id.replace("_", " ").title()

    def relationships(self) -> list[Relationship]:
        return self._relationships(self)

    def _relationships(self, to: BaseModel) -> list[Relationship]:
        relationships = []
        for field_name, field in to.model_fields.items():
            field_value = getattr(to, field_name)
            if not field_value:
                continue
            if isinstance(field_value, CDFSequence):
                relationships.append(self._create_relationship(field_value.external_id, "SEQUENCE", field_name))
            elif isinstance(field_value, TimeSeries):
                relationships.append(self._create_relationship(field_value.external_id, "TIMESERIES", field_name))
            elif isinstance(field_value, list) and field_value and isinstance(field_value[0], TimeSeries):
                # The string comparison is to avoid circular imports
                if type(to).__name__ == "ProductionPlanTimeSeries" and field_name == "series":
                    # For some reason, the production plan time series are not linked to the benchmarking config
                    continue

                relationships.extend(
                    self._create_relationship(ts.external_id, "TIMESERIES", field_name) for ts in field_value
                )
            elif isinstance(field_value, list) and field_value and isinstance(field_value[0], CDFSequence):
                relationships.extend(
                    self._create_relationship(sequence.external_id, "SEQUENCE", field_name) for sequence in field_value
                )
            elif isinstance(field_value, AssetType) or (
                isinstance(field_value, list) and field_value and isinstance(field_value[0], AssetType)
            ):
                asset_types: list[AssetType] = field_value if isinstance(field_value, list) else [field_value]
                for target in asset_types:
                    target_type = target.type_
                    if self.type_ == "plant" and target.type_ == "reservoir":
                        target_type = "inlet_reservoir"
                    relationships.append(self._create_relationship(target.external_id, "ASSET", target_type))
            elif isinstance(field_value, NonAssetType) or (
                isinstance(field_value, list) and field_value and isinstance(field_value[0], NonAssetType)
            ):
                non_asset_types: list[NonAssetType] = field_value if isinstance(field_value, list) else [field_value]
                for target in non_asset_types:
                    relationships.extend(self._relationships(target))

        return relationships

    def _create_relationship(
        self,
        target_external_id: str,
        target_cdf_type: str,
        target_type: str,
    ) -> Relationship:
        source_type = "ASSET"

        # The market model uses the suffix CDF type for the relationship label, while the core model does not.
        try:
            # Production Model
            label = RelationshipLabel(f"relationship_to.{target_type}")
        except ValueError:
            # Market Model
            label = RelationshipLabel(f"relationship_to.{target_type}_{target_cdf_type.lower()}")
            # In addition, the Market Model uses capitalised CDF types for the relationship type,
            # while the core model uses all upper cases.
            source_type = source_type.title()
            target_cdf_type = target_cdf_type.title()

        return Relationship(
            external_id=f"{self.external_id}.{target_external_id}",
            source_external_id=self.external_id,
            source_type=source_type,
            target_external_id=target_external_id,
            target_type=target_cdf_type,
            labels=[Label(external_id=label.value)],
        )

    def as_asset(self):
        metadata = {}
        for field_name, field in self.model_fields.items():
            if (
                any(cdf_type in str(field.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__])
                or field_name in {"name", "description", "label", "parent_external_id"}
                or field.exclude
            ):
                continue
            value = getattr(self, field_name)
            if value is None or isinstance(value, AssetType) or isinstance_list(value, AssetType):
                continue
            if isinstance(value, list) and not value:
                continue
            if isinstance(value, NonAssetType):
                value = value.model_dump(exclude_unset=True)
                for k, v in value.items():
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v)
                    metadata[f"{field_name}:{k}"] = v
                continue

            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            metadata[field_name] = value

        return Asset(
            external_id=self.external_id,
            name=self.name,
            parent_external_id=self.parent_external_id,
            labels=[Label(self.label.value)],
            metadata=metadata or None,
            description=self.description,
        )

    @classmethod
    def _parse_asset_metadata(cls, metadata: dict[str, str] | None = None) -> dict[str, dict | str]:
        if not metadata:
            return {}
        output: dict[str, dict | str] = {}
        for key, value in metadata.items():
            counts = Counter(key)
            if counts[":"] == 1:
                field_name, sub_key = key.split(":")
                if field_name not in output:
                    output[field_name] = {}
                output[field_name][sub_key] = value
            elif counts[":"] == 0:
                output[key] = value
            else:
                raise NotImplementedError("Nested keys more than one level is not supported")

        return output

    @classmethod
    def _from_asset(
        cls,
        asset: Asset,
        additional_fields: Optional[dict[str, Any]] = None,
    ) -> T_Asset_Type:
        if not additional_fields:
            additional_fields = {}
        metadata = cls._parse_asset_metadata(asset.metadata)
        instance = cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            **metadata,
            **additional_fields,
        )
        AssetType._instantiated_assets[asset.external_id] = instance
        return instance

    @classmethod
    def _field_name_asset_resource_class(cls) -> Iterable[tuple[str, TypingType[AssetType]]]:
        """AssetType fields are are of type list[AssetType] or Optional[AssetType]"""
        # todo? method is identical to _field_name_asset_resource_class in AssetModel
        # * unsure how to reuse?
        for field_name in cls.model_fields:
            class_ = cls.model_fields[field_name].annotation
            if pydantic_model_class_candidate(class_):
                asset_resource_class = get_args(class_)[0]
                if issubclass(asset_resource_class, AssetType):
                    yield field_name, asset_resource_class

    @classmethod
    def _fetch_and_set_metadata(
        cls,
        client: CogniteClient,
        data_set_id: int,
        additional_fields: dict[str, Union[list, None]],
        asset_external_id: str,
        fetch_metadata: bool,
        fetch_content: bool,
    ) -> dict[str, Any]:
        """Fetches resources that are linked with relationships to the asset."""
        relationships = client.relationships.list(
            source_external_ids=[asset_external_id],
            source_types=["asset"],
            target_types=["timeseries", "asset", "sequence", "file"],
            data_set_ids=[data_set_id],
            limit=-1,
        )
        for r in relationships:
            field = match_field_from_relationship(cls.model_fields, r)
            target_type = r.target_type.lower()

            if target_type == "asset":
                if r.target_external_id in AssetType._instantiated_assets:
                    relationship_target = AssetType._instantiated_assets[r.target_external_id]

                else:
                    target_class = [y for x, y in cls._field_name_asset_resource_class() if field == x and y][0]
                    relationship_target = target_class.from_cdf(
                        client=client,
                        data_set_id=data_set_id,
                        external_id=r.target_external_id,
                        fetch_metadata=fetch_metadata,
                        fetch_content=fetch_content,
                    )

            elif target_type == "timeseries":
                relationship_target = client.time_series.retrieve(external_id=r.target_external_id)
            elif target_type == "sequence":
                try:
                    relationship_target = CDFSequence.from_cdf(client, r.target_external_id, fetch_content)
                except CogniteNotFoundError:
                    relationship_target = None
            elif target_type == "file":
                try:
                    relationship_target = CDFFile.from_cdf(client, r.target_external_id, fetch_content)
                except CogniteNotFoundError:
                    relationship_target = None
            else:
                raise ValueError(f"Cannot handle target type {r.target_type}")

            # Add relationship target to additional fields in-place
            if isinstance(additional_fields[field], list):
                additional_fields[field].append(relationship_target)
            else:
                additional_fields[field] = relationship_target

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        data_set_id: int,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> T_Asset_Type:
        """
        Fetch an asset from CDF and convert it to a model instance.
        Optionally fetch relationships targets and content by setting
        `fetch_metadata` and optionally `fetch_content`

        By default, content of files/sequences/time series is not fetched.
        This can be enabled by setting `fetch_content=True`.
        """

        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            # Check if asset has already been instantiated, eg. by a relationship
            if external_id in AssetType._instantiated_assets:
                return AssetType._instantiated_assets[external_id]
            else:
                asset = client.assets.retrieve(
                    external_id=external_id,
                )
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")

        # Prepare non-asset metadata fields
        additional_fields = {
            field: [] if "list" in str(field_info.annotation) else None
            for field, field_info in cls.model_fields.items()
            if field in [x for x, _ in cls._field_name_asset_resource_class()]
            or any(cdf_type in str(field_info.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__])
        }

        # Populate non-asset metadata fields, according to relationships/flags
        # `Additional_fields`, is modified in-place by `_fetch_metadata`
        if fetch_metadata:
            cls._fetch_and_set_metadata(
                client,
                data_set_id,
                additional_fields,
                asset.external_id,
                fetch_metadata,
                fetch_content,
            )
        instance = cls._from_asset(asset, additional_fields)
        if asset.external_id != instance.external_id:
            # The external id is not set in a standardized way for Market configs.
            instance.external_id = asset.external_id
        return instance

    def _asset_type_prepare_for_diff(self: T_Asset_Type) -> dict[str, dict]:
        for model_field in self.model_fields:
            field = getattr(self, model_field)
            if isinstance(field, AssetType):
                # Only include external id in diff
                setattr(self, model_field, field.external_id)

            elif isinstance_list(field, AssetType):
                # Sort bt external id to have consistent order for diff
                setattr(self, model_field, sorted(map(lambda x: x.external_id, field)))
            elif isinstance(field, Path):
                # remove path from diff
                setattr(self, model_field, None)
        return self


T_Asset_Type = TypeVar("T_Asset_Type", bound=AssetType)


class NonAssetType(BaseModel, ABC):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)


class Model(BaseModel, ABC):
    def sequences(self) -> list[CDFSequence]:
        sequences = [sequence for item in self._resource_types() for sequence in item.sequences()]
        sequences.extend(self._fields_of_type(CDFSequence))
        return sequences

    def files(self) -> list[CDFFile]:
        files = [file for item in self._resource_types() for file in item.files()]
        files.extend(self._fields_of_type(CDFFile))
        return files

    def time_series(self) -> list[TimeSeries]:
        time_series = [ts for item in self._resource_types() for ts in item.time_series()]
        time_series.extend(self._fields_of_type(TimeSeries))
        return time_series

    def drop_content(self) -> None:
        for item in self._resource_types():
            for sequence in item.sequences():
                sequence.content = None
            for file in item.files():
                file.content = None

    def _resource_types(self) -> Iterable[ResourceType]:
        for f in self.model_fields:
            if isinstance(items := getattr(self, f), list) and items and isinstance(items[0], ResourceType):
                yield from items

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

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary: dict[str, dict[str, dict[str, int]]] = {self.model_name: {"domain": {}, "cdf": {}}}
        summary[self.model_name]["domain"] = {
            field_name: len(value) if isinstance(value := getattr(self, field_name), (list, dict)) else 1
            for field_name in self.model_fields
        }
        summary[self.model_name]["cdf"]["files"] = len(self.files())
        summary[self.model_name]["cdf"]["sequences"] = len(self.sequences())
        summary[self.model_name]["cdf"]["time_series"] = len(self.time_series())
        return summary

    def dump_external_ids(self) -> dict[str, dict, str, dict[str, list[str]]]:
        output: dict[str, dict, str, dict[str, list[str]]] = {self.model_name: {"domain": {}, "cdf": {}}}
        output[self.model_name]["domain"] = {
            field_name: [(v.external_id if hasattr(v, "external_id") else str(v)) for v in value]
            if isinstance(value := getattr(self, field_name), (list, dict))
            else [value.external_id if hasattr(value, "external_id") else str(value)]
            for field_name in self.model_fields
        }
        output[self.model_name]["cdf"]["files"] = [file.external_id for file in self.files()]
        output[self.model_name]["cdf"]["sequences"] = [sequence.external_id for sequence in self.sequences()]
        output[self.model_name]["cdf"]["time_series"] = [time_series.external_id for time_series in self.time_series()]
        return output

    def summary_diff(self: T_Model, other: T_Model) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
        this_summary = self.dump_external_ids()
        other_summary = other.dump_external_ids()
        output: dict[str, dict[str, dict[str, dict[str, int]]]] = {}
        for model_name in this_summary:
            output[model_name] = {}
            for domain in this_summary[model_name]:
                output[model_name][domain] = {}
                for type_ in this_summary[model_name][domain]:
                    this_ext_ids = set(this_summary[model_name][domain][type_])
                    other_ext_ids = set(other_summary[model_name][domain][type_])
                    if not type_ == "nodes":
                        output[model_name][domain][type_] = {
                            "added": len(this_ext_ids - other_ext_ids),
                            "removed": len(other_ext_ids - this_ext_ids),
                        }
                        continue
                    # Todo this is a hack go get more detailed diff for nodes
                    # To make it not a hack the external id has to be standardized
                    # and not just assumed to have this format.
                    output[model_name][domain][type_] = {}

                    this_ext_ids_by_node_type = defaultdict(set)
                    for node_type, ext_id in groupby(sorted(this_ext_ids), key=lambda x: x.split("_", 1)[0]):
                        this_ext_ids_by_node_type[node_type].update(ext_id)
                    other_ext_ids_by_node_type = defaultdict(set)
                    for node_type, ext_id in groupby(sorted(other_ext_ids), key=lambda x: x.split("_", 1)[0]):
                        other_ext_ids_by_node_type[node_type].update(ext_id)
                    for node_type in set(this_ext_ids_by_node_type) | set(other_ext_ids_by_node_type):
                        if node_type != "BM":
                            output[model_name][domain][type_][node_type] = {
                                "added": len(
                                    this_ext_ids_by_node_type[node_type] - other_ext_ids_by_node_type[node_type]
                                ),
                                "removed": len(
                                    other_ext_ids_by_node_type[node_type] - this_ext_ids_by_node_type[node_type]
                                ),
                            }
                            continue
                        output[model_name][domain][type_][node_type] = {}
                        this_mapping_by_watercourse = defaultdict(set)
                        for watercourse, ext_id in groupby(
                            sorted(this_ext_ids_by_node_type[node_type]), lambda x: x.split("__")[1]
                        ):
                            this_mapping_by_watercourse[watercourse].update(ext_id)
                        other_mapping_by_watercourse = defaultdict(set)
                        for watercourse, ext_id in groupby(
                            sorted(other_ext_ids_by_node_type[node_type]), lambda x: x.split("__")[1]
                        ):
                            other_mapping_by_watercourse[watercourse].update(ext_id)
                        for watercourse in set(this_mapping_by_watercourse) | set(other_mapping_by_watercourse):
                            output[model_name][domain][type_][node_type][watercourse] = {
                                "added": len(
                                    this_mapping_by_watercourse[watercourse] - other_mapping_by_watercourse[watercourse]
                                ),
                                "removed": len(
                                    other_mapping_by_watercourse[watercourse] - this_mapping_by_watercourse[watercourse]
                                ),
                            }
        return output

    def dump(self) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if time_series := self.time_series():
            output["time_series"] = sorted(
                ({"external_id": ts.external_id} if isinstance(ts, TimeSeries) else ts for ts in time_series),
                key=self._external_id_key,
            )
        if files := self.files():
            output["files"] = sorted(
                (remove_read_only_fields(file.dump(camel_case=False)) for file in files),
                key=self._external_id_key,
            )
        if sequences := self.sequences():

            def dump_sequence(resource: CDFSequence) -> dict[str, Any]:
                output = remove_read_only_fields(resource.dump(camel_case=False))
                if (columns := output.get("columns")) and isinstance(columns, list):
                    for no, column in enumerate(columns):
                        output["columns"][no] = remove_read_only_fields(column)
                return output

            output["sequences"] = sorted(
                (dump_sequence(sequence) for sequence in sequences),
                key=self._external_id_key,
            )
        return output

    @classmethod
    def load(cls: TypingType[T_Model], data: dict[str, Any]) -> T_Model:
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
        data_set_id: int,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> T_Model:
        ...

    def difference(self: T_Model, other: T_Model, print_string: bool = True) -> dict:
        if type(self) != type(other):
            raise ValueError("Cannot compare these models of different types.")

        self_dump = self._prepare_for_diff()
        other_dump = other._prepare_for_diff()
        diff_dict = {}
        for model_field in self_dump:
            if deep_diff := DeepDiff(
                self_dump[model_field],
                other_dump[model_field],
                ignore_type_in_groups=[(float, int, type(None))],
                exclude_regex_paths=[
                    r"(.+?)._cognite_client",
                    r"(.+?).last_updated_time",
                    r"(.+?).parent_id",
                    r"(.+?).root_id]",
                    r"(.+?).data_set_id",
                    r"(.+?).created_time",
                    r"(.+?)lastUpdatedTime",
                    r"(.+?)createdTime",
                    r"(.+?)parentId",
                    r"(.+?)\.id",
                    # Relevant metadata should already be included in the model
                    r"(.+?)metadata",
                ],
            ).to_dict():
                diff_dict[model_field] = deep_diff

        if print_string:
            _diff_formatter = _DiffFormatter(
                full_diff_per_field=diff_dict,
                model_a=self_dump,
                model_b=other_dump,
            )
            print(_diff_formatter.format_as_string())

        return diff_dict

    def difference_external_ids(self: T_Model, other: T_Model) -> dict:
        this_summary = self.dump_external_ids()
        other_summary = other.dump_external_ids()
        output: dict[str, dict[str, dict[str, dict[str, list[str]]]]] = {}
        for model_name in this_summary:
            output[model_name] = {}
            for domain in this_summary[model_name]:
                output[model_name][domain] = {}
                for type_ in this_summary[model_name][domain]:
                    this_ext_ids = set(this_summary[model_name][domain][type_])
                    other_ext_ids = set(other_summary[model_name][domain][type_])
                    if not type_ == "nodes":
                        added = sorted(this_ext_ids - other_ext_ids)
                        removed = sorted(other_ext_ids - this_ext_ids)
                        output[model_name][domain][type_] = {
                            "added": added[: min(len(added), 10)],
                            "removed": removed[: min(len(removed), 10)],
                        }
                        continue
                    # Todo this is a hack go get more detailed diff for nodes
                    # To make it not a hack the external id has to be standardized
                    # and not just assumed to have this format.
                    output[model_name][domain][type_] = {}

                    this_ext_ids_by_node_type = defaultdict(set)
                    for node_type, ext_id in groupby(sorted(this_ext_ids), key=lambda x: x.split("_", 1)[0]):
                        this_ext_ids_by_node_type[node_type].update(ext_id)
                    other_ext_ids_by_node_type = defaultdict(set)
                    for node_type, ext_id in groupby(sorted(other_ext_ids), key=lambda x: x.split("_", 1)[0]):
                        other_ext_ids_by_node_type[node_type].update(ext_id)
                    for node_type in set(this_ext_ids_by_node_type) | set(other_ext_ids_by_node_type):
                        if node_type != "BM":
                            added = sorted(this_ext_ids_by_node_type[node_type] - other_ext_ids_by_node_type[node_type])
                            removed = sorted(
                                other_ext_ids_by_node_type[node_type] - this_ext_ids_by_node_type[node_type]
                            )
                            output[model_name][domain][type_][node_type] = {
                                "added": added[: min(len(added), 10)],
                                "removed": removed[: min(len(removed), 10)],
                            }
                            continue
                        output[model_name][domain][type_][node_type] = {}
                        this_mapping_by_watercourse = defaultdict(set)
                        for watercourse, ext_id in groupby(
                            sorted(this_ext_ids_by_node_type[node_type]), lambda x: x.split("__")[1]
                        ):
                            this_mapping_by_watercourse[watercourse].update(ext_id)
                        other_mapping_by_watercourse = defaultdict(set)
                        for watercourse, ext_id in groupby(
                            sorted(other_ext_ids_by_node_type[node_type]), lambda x: x.split("__")[1]
                        ):
                            other_mapping_by_watercourse[watercourse].update(ext_id)
                        for watercourse in set(this_mapping_by_watercourse) | set(other_mapping_by_watercourse):
                            added = sorted(
                                this_mapping_by_watercourse[watercourse] - other_mapping_by_watercourse[watercourse]
                            )
                            removed = sorted(
                                other_mapping_by_watercourse[watercourse] - this_mapping_by_watercourse[watercourse]
                            )

                            output[model_name][domain][type_][node_type][watercourse] = {
                                "added": added[: min(len(added), 10)],
                                "removed": removed[: min(len(removed), 10)],
                            }
        return output


T_Model = TypeVar("T_Model", bound=Model)


class AssetModel(Model, ABC):
    root_asset: ClassVar[Optional[Asset]] = None

    def assets(self) -> list[Asset]:
        return [item.as_asset() for item in self._asset_types()]

    def relationships(self) -> list[Relationship]:
        return [edge for item in self._asset_types() for edge in item.relationships()]

    def parent_assets(self) -> list[Asset]:
        if not self.root_asset:
            return []

        def _to_name(external_id: str) -> str:
            parts = external_id.replace("_", " ").split(" ")
            if parts[0].lower() == "rkom":
                parts[0] = parts[0].upper()
            else:
                parts[0] = parts[0].title()
            # The replacing is to have avoid doing changes, even though this is inconsistent with the other parent
            # assets.
            return " ".join(parts).replace("Bid process", "Bid").replace("bid process", "bid")

        parent_and_description_ids = {
            (item.parent_external_id, item.parent_description or "") for item in self._asset_types()
        }

        return [self.root_asset] + [
            Asset(
                external_id=parent_id,
                name=_to_name(parent_id),
                parent_external_id=self.root_asset.external_id,
                description=description,
            )
            for parent_id, description in parent_and_description_ids
        ]

    def _asset_types(self) -> Iterable[AssetType]:
        yield from (item for item in self._resource_types() if isinstance(item, AssetType))

    @classmethod
    def _field_name_asset_resource_class(cls) -> Iterable[tuple[str, TypingType[AssetType]]]:
        """AssetType fields are of type list[AssetType] or Optional[AssetType]"""
        # todo? method is identical to _field_name_asset_resource_class in AssetType
        # * unsure how to reuse?
        for field_name in cls.model_fields:
            class_ = cls.model_fields[field_name].annotation
            if pydantic_model_class_candidate(class_):
                asset_resource_class = get_args(class_)[0]
                if (is_asset_type := issubclass(asset_resource_class, AssetType)) and any(
                    base is abc.ABC for base in asset_resource_class.__bases__
                ):
                    for subclass in all_subclasses(asset_resource_class):
                        yield field_name, subclass
                elif is_asset_type:
                    yield field_name, asset_resource_class

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary = super().summary()
        summary[self.model_name]["cdf"]["assets"] = len(self.assets())
        summary[self.model_name]["cdf"]["relationships"] = len(self.relationships())
        summary[self.model_name]["cdf"]["parent_assets"] = len(self.parent_assets())
        return summary

    def dump_external_ids(self) -> dict[str, dict, str, dict[str, list[str]]]:
        output = super().dump_external_ids()
        output[self.model_name]["cdf"]["assets"] = [asset.external_id for asset in self.assets()]
        output[self.model_name]["cdf"]["relationships"] = [
            relationship.external_id for relationship in self.relationships()
        ]
        output[self.model_name]["cdf"]["parent_assets"] = [asset.external_id for asset in self.parent_assets()]
        return output

    def dump(self) -> dict[str, Any]:
        output = super().dump()
        for field_method in [self.assets, self.relationships, self.parent_assets]:
            if resources := field_method():
                output[field_method.__name__] = sorted(
                    (remove_read_only_fields(resource.dump(camel_case=False)) for resource in resources),
                    key=self._external_id_key,
                )
        return output

    @classmethod
    def from_cdf(
        cls: TypingType[T_Asset_Model],
        client: PowerOpsClient,
        data_set_id: int,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> T_Asset_Model:
        if fetch_content and not fetch_metadata:
            raise ValueError("Cannot fetch content without also fetching metadata")
        cdf = client.cdf
        output = defaultdict(list)
        for field_name, asset_cls in cls._field_name_asset_resource_class():
            try:
                assets = cdf.assets.retrieve_subtree(external_id=asset_cls.parent_external_id)
            except AttributeError:
                raise
            for asset in assets:
                if asset.external_id == asset_cls.parent_external_id:
                    continue
                instance = asset_cls.from_cdf(
                    client=cdf,
                    data_set_id=data_set_id,
                    asset=asset,
                    fetch_metadata=fetch_metadata,
                    fetch_content=fetch_content,
                )
                output[field_name].append(instance)

        return cls(**output)

    def _prepare_for_diff(self: T_Asset_Model) -> dict[str:dict]:
        clone = self.model_copy(deep=True)

        for model_field in clone.model_fields:
            field_value = getattr(clone, model_field)
            if isinstance_list(field_value, AssetType):
                # Sort the asset types to have comparable order for diff
                _sorted = sorted(field_value, key=lambda x: x.external_id)
                # Prepare each asset type for diff
                _prepared = map(lambda x: x._asset_type_prepare_for_diff(), _sorted)
                setattr(clone, model_field, list(_prepared))
            elif isinstance(field_value, AssetType):
                field_value._asset_type_prepare_for_diff()
        # Some fields are have been set to their external_id which gives a warning we can ignore
        return clone.model_dump(warnings=False)

    def difference(self: T_Asset_Model, other: T_Asset_Model, print_string: bool = True) -> dict:
        if type(self) != type(other):
            raise ValueError("Cannot compare these models of different types.")

        self_dump = self._prepare_for_diff()
        other_dump = other._prepare_for_diff()
        diff_dict = {}
        for model_field in self_dump:
            if deep_diff := DeepDiff(
                self_dump[model_field],
                other_dump[model_field],
                ignore_type_in_groups=[(float, int, type(None))],
                exclude_regex_paths=[
                    r"(.+?)._cognite_client",
                    r"(.+?).last_updated_time",
                    r"(.+?).parent_id",
                    r"(.+?).root_id]",
                    r"(.+?).data_set_id",
                    r"(.+?).created_time",
                    r"(.+?)lastUpdatedTime",
                    r"(.+?)createdTime",
                    r"(.+?)parentId",
                    r"(.+?)\.id",
                    # Relevant metadata should already be included in the model
                    r"(.+?)metadata",
                ],
            ).to_dict():
                diff_dict[model_field] = deep_diff

        if print_string:
            _diff_formatter = _DiffFormatter(
                full_diff_per_field=diff_dict,
                model_a=self_dump,
                model_b=other_dump,
            )
            print(_diff_formatter.format_as_string())

        return diff_dict


T_Asset_Model = TypeVar("T_Asset_Model", bound=AssetModel)


class DataModel(Model, ABC):
    def instances(self) -> InstancesApply:
        nodes: dict[str, NodeApply] = {}
        edges: dict[str, EdgeApply] = {}
        for domain_model in self._domain_models():
            instance_applies = domain_model.to_instances_apply()
            # Caching in case recursive relationships are used.
            for node in instance_applies.nodes:
                if node.external_id not in nodes:
                    nodes[node.external_id] = node
            for edge in instance_applies.edges:
                if edge.external_id not in edges:
                    edges[edge.external_id] = edge

        return InstancesApply(nodes=NodeApplyList(nodes.values()), edges=EdgeApplyList(edges.values()))

    def _domain_models(self) -> Iterable[DomainModelApply]:
        for field_name in self.model_fields:
            items = getattr(self, field_name)
            if isinstance(items, list) and items and isinstance(items[0], (DomainModelApply, DomainModelApplyCogShop1)):
                yield from items
            if isinstance(items, (DomainModelApply, DomainModelApplyCogShop1)):
                yield items
            if (
                isinstance(items, dict)
                and items
                and isinstance(next(iter(items.values())), (DomainModelApply, DomainModelApplyCogShop1))
            ):
                yield from items.values()

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary = super().summary()
        instances = self.instances()
        summary[self.model_name]["cdf"]["nodes"] = len(instances.nodes)
        summary[self.model_name]["cdf"]["edges"] = len(instances.edges)
        return summary

    def dump_external_ids(self) -> dict[str, dict, str, dict[str, list[str]]]:
        output = super().dump_external_ids()
        instances = self.instances()
        output[self.model_name]["cdf"]["nodes"] = [node.external_id for node in instances.nodes]
        output[self.model_name]["cdf"]["edges"] = [edge.external_id for edge in instances.edges]
        return output

    def dump(self) -> dict[str, Any]:
        output = super().dump()
        instances = self.instances()
        for instance_type in ["nodes", "edges"]:
            if resources := getattr(instances, instance_type):
                output[instance_type] = sorted(
                    (remove_read_only_fields(resource.dump(camel_case=False)) for resource in resources),
                    key=self._external_id_key,
                )
        return output


class _DiffFormatter:
    def __init__(self, full_diff_per_field: dict[str, dict], model_a: dict, model_b: dict):
        self.full_diff_per_field = full_diff_per_field
        self.model_a = model_a
        self.model_b = model_b

        self.str_builder: list = None

    def _format_per_field(self, field_name: str, field_diff: dict[str, Union[dict, PrettyOrderedSet]]):
        self.str_builder.extend(
            (
                "\n\n========================== ",
                *field_name.title().split("_"),
                " ==========================\n",
            )
        )
        # Might need a better fallback for names
        self.str_builder.append("Indexes and names:\n\t")
        names = [
            f'{i}:{d.get("display_name", False) or d.get("name", "")}, ' for i, d in enumerate(self.model_a[field_name])
        ]

        self.str_builder.extend(names)
        self.str_builder.append("\n\n")

        for diff_type, diffs in field_diff.items():
            is_iterable = "iterable" in diff_type

            if diff_type in ("type_changes", "values_changed"):
                self.str_builder.extend(
                    (
                        f'The following values have changed {"type and value" if "type" in diff_type else ""}:\n',
                        *format_change_binary(diffs),
                        "\n",
                    ),
                )
            elif "removed" in diff_type:
                self.str_builder.extend(
                    (
                        f"The following {'values' if is_iterable else 'entries'} have been removed:\n",
                        *format_value_removed(diffs),
                        "\n",
                    )
                )
            elif "added" in diff_type:
                self.str_builder.extend(
                    (
                        f"The following {'values' if is_iterable else 'entries'} have been added:\n",
                        *format_value_added(diffs, self.model_b[field_name]),
                        "\n",
                    )
                )

            else:
                print(f"cannot handle {diff_type=}")

    def format_as_string(self) -> str:
        self.str_builder = []
        for field_name, field_diff in self.full_diff_per_field.items():
            self._format_per_field(field_name, field_diff)

        return "".join(self.str_builder)
