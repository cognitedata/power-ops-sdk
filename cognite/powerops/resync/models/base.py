from __future__ import annotations

import abc
from collections import defaultdict, Counter
from itertools import groupby
import json
from abc import ABC

from deepdiff import DeepDiff
from deepdiff.model import PrettyOrderedSet

from pathlib import Path
from typing import Any, ClassVar, Iterable, Optional, Union, TypeVar, Callable
from typing import Type as TypingType
from pydantic import BaseModel
from typing_extensions import Self
from cognite.client.data_classes import Asset, Label, Relationship, TimeSeries, AssetList
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
)
from cognite.powerops.resync.utils.serializer import remove_read_only_fields, get_pydantic_annotation

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


class AssetType(ResourceType, ABC, arbitrary_types_allowed=True):
    parent_external_id: ClassVar[str]
    label: ClassVar[Union[AssetLabel, str]]
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

    def as_asset(self) -> Asset:
        metadata = self._as_metadata()

        return Asset(
            external_id=self.external_id,
            name=self.name,
            parent_external_id=self.parent_external_id,
            labels=[Label(str(self.label.value))],
            metadata=metadata or None,
            description=self.description,
        )

    def _as_metadata(self) -> dict[str, str]:
        metadata = {}
        for field_name, field in self.model_fields.items():
            annotation, _ = get_pydantic_annotation(field.annotation)
            if (
                annotation in [CDFSequence, TimeSeries, CDFFile]
                or issubclass(annotation, AssetType)
                or field_name in {"name", "description", "label", "parent_external_id"}
                or field.exclude
            ):
                continue

            value = getattr(self, field_name)
            if isinstance(value, NonAssetType):
                value = value.model_dump(exclude_unset=True)
                for k, v in value.items():
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v)
                    metadata[f"{field_name}:{k}"] = v
            elif isinstance(value, (dict, list)) and value:
                metadata[field_name] = json.dumps(value)
            elif isinstance(value, (dict, list)) and not value:
                continue
            elif field.annotation in (str, int, float):
                metadata[field_name] = value
            else:
                raise NotImplementedError(f"Cannot handle metadata of type {field.annotation}")
        return metadata

    @classmethod
    def from_asset(cls, asset: Asset) -> Self:
        instance = cls(
            name=asset.name,
            description=asset.description,
            **cls._load_metadata(asset.metadata),
        )
        if instance.external_id != asset.external_id:
            # The external id is not set in a standardized way for Market configs.
            instance.external_id = asset.external_id
        return instance

    @classmethod
    def _load_metadata(cls, metadata: dict[str, str] | None = None) -> dict[str, dict | str]:
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

    def sort_listed_asset_types(self) -> None:
        for field_name, field in self.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation)
            if issubclass(annotation, AssetType) and outer is list:
                getattr(self, field_name).sort(key=lambda x: x.external_id)

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


class NonAssetType(BaseModel, ABC, arbitrary_types_allowed=True):
    ...


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
                        output = remove_read_only_fields(resource.dump(camel_case=False))
                        if (columns := output.get("columns")) and isinstance(columns, list):
                            for no, column in enumerate(columns):
                                output["columns"][no] = remove_read_only_fields(column)
                        return output

                else:

                    def dump(resource: Any) -> dict[str, Any]:
                        return remove_read_only_fields(resource.dump(camel_case=False))

                output[name] = sorted((dump(item) for item in items), key=self._external_id_key)
        return output

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary: dict[str, dict[str, dict[str, int]]] = {self.model_name: {"domain": {}, "cdf": {}}}
        summary[self.model_name]["domain"] = {
            field_name: len(value) if isinstance(value := getattr(self, field_name), (list, dict)) else 1
            for field_name in self.model_fields
        }
        summary[self.model_name]["cdf"]["files"] = len(self.files())
        summary[self.model_name]["cdf"]["sequences"] = len(self.sequences())
        summary[self.model_name]["cdf"]["time_series"] = len(self.timeseries())
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
        output[self.model_name]["cdf"]["time_series"] = [time_series.external_id for time_series in self.timeseries()]
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

    @classmethod
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

    def parent_assets(cls, include_root: bool = True) -> AssetList:
        if not cls.root_asset:
            return AssetList([])

        def _to_name(external_id: str) -> str:
            parts = external_id.replace("_", " ").split(" ")
            if parts[0].lower() == "rkom":
                parts[0] = parts[0].upper()
            else:
                parts[0] = parts[0].title()
            # The replacing is to have avoid doing changes, even though this is inconsistent with the other parent
            # assets.
            return " ".join(parts).replace("Bid process", "Bid").replace("bid process", "bid")

        parent_and_description_ids = set()
        for field_name, field in cls.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation)
            if issubclass(annotation, AssetType) and outer is list:
                parent_and_description_ids.add((annotation.parent_external_id, annotation.parent_description or ""))

        return AssetList(
            ([cls.root_asset] if include_root else [])
            + [
                Asset(
                    external_id=parent_id,
                    name=_to_name(parent_id),
                    parent_external_id=cls.root_asset.external_id,
                    description=description,
                )
                for parent_id, description in parent_and_description_ids
            ]
        )

    cdf_resources: ClassVar[dict[Union[Callable, tuple[Callable, str]], type]] = {
        **dict(Model.cdf_resources.items()),
        assets: Asset,
        relationships: Relationship,
        parent_assets: Asset,
    }
    # Need to set classmethod here to have access to the underlying function in cdf_resources
    parent_assets = classmethod(parent_assets)

    def _asset_types(self) -> Iterable[AssetType]:
        yield from (item for item in self._resource_types() if isinstance(item, AssetType))

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

    @classmethod
    def load_from_cdf_resources(cls: TypingType[Self], data: dict[str, Any]) -> Self:
        loaded_by_type_external_id: dict[str, dict[str, Any]] = {}
        for function, resource_cls in cls.cdf_resources.items():
            if isinstance(function, tuple):
                name = function[1]
            else:
                name = function.__name__
            if items := data.get(name):
                loaded_by_type_external_id[name] = {
                    loaded.external_id: loaded
                    for loaded in (resource_cls._load(item) if isinstance(item, dict) else item for item in items)
                }

        parsed = cls._create_cls_arguments(loaded_by_type_external_id)

        instance = cls(**parsed)

        cls._set_linked_resources(loaded_by_type_external_id)
        return instance

    @classmethod
    def _create_cls_arguments(cls, loaded_by_type_external_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
        asset_by_parent_external_id = defaultdict(list)
        for asset in loaded_by_type_external_id["assets"].values():
            asset_by_parent_external_id[asset.parent_external_id].append(asset)
        arguments = {}
        asset_type_by_external_id = {}
        for field_name, field in cls.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation)
            if issubclass(annotation, AssetType) and (
                assets := asset_by_parent_external_id.get(annotation.parent_external_id)
            ):
                if outer is list:
                    arguments[field_name] = [annotation.from_asset(asset) for asset in assets]
                    asset_type_by_external_id.update({asset.external_id: asset for asset in arguments[field_name]})
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()
        loaded_by_type_external_id["assets"] = asset_type_by_external_id
        return arguments

    @classmethod
    def _set_linked_resources(cls, loaded_by_type_external_id: dict[str, dict[str, Any]]) -> None:
        relationships_by_source_external_id = defaultdict(list)
        for relationship in loaded_by_type_external_id["relationships"].values():
            relationships_by_source_external_id[relationship.source_external_id].append(relationship)

        for source_id, relationships in relationships_by_source_external_id.items():
            if not (source := loaded_by_type_external_id["assets"].get(source_id)):
                # Todo print warning
                # Missing source
                continue

            for relationship in relationships:
                target_type = relationship.target_type.casefold()
                for key in (target_type + "s", target_type):
                    if target_items := loaded_by_type_external_id.get(key):
                        break
                else:
                    # Todo print warning
                    # Missing target type
                    continue
                if not (target := target_items.get(relationship.target_external_id)):
                    # Todo print warning
                    # Missing target external id
                    continue

                field_name = relationship.labels[0].external_id.split(".")[1]
                if field_name not in source.model_fields:
                    field_name += "s"
                if field_name not in source.model_fields:
                    raise ValueError(f"Cannot find field {field_name} in {source}")

                annotation, outer = get_pydantic_annotation(source.model_fields[field_name].annotation)
                if outer is list:
                    getattr(source, field_name).append(target)
                elif outer is dict:
                    getattr(source, field_name)[target.external_id] = target
                elif outer is None:
                    setattr(source, field_name, target)
                else:
                    raise NotImplementedError()

    def sort_listed_asset_types(self) -> None:
        for field_name, field in self.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation)
            if issubclass(annotation, AssetType) and outer is list:
                getattr(self, field_name).sort(key=lambda x: x.external_id)
                for asset_type in getattr(self, field_name):
                    asset_type.sort_listed_asset_types()

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
        parent_assets = cls.parent_assets(include_root=False)

        assets = cdf.assets.list(asset_subtree_external_ids=parent_assets.as_external_ids(), limit=-1)
        relationships = cdf.relationships.list(
            source_external_ids=assets.as_external_ids(),
            source_types=["asset"],
            target_types=["timeseries", "asset", "sequence"],
            data_set_ids=[data_set_id],
            limit=-1,
        )
        # TimeSeries are a special case as resync are only referring to them by external id.
        # So we do not need to fetch them
        time_series = [
            TimeSeries(external_id=relationship.target_external_id)
            for relationship in relationships
            if relationship.target_type.casefold() == "timeseries"
        ]
        sequence_ids = [
            relationship.target_external_id
            for relationship in relationships
            if relationship.target_type.casefold() == "sequence"
        ]
        sequences = cdf.sequences.retrieve_multiple(external_ids=sequence_ids)
        return cls.load_from_cdf_resources(
            {
                "assets": assets,
                "relationships": relationships,
                "sequences": sequences,
                "time_series": time_series,
            }
        )

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

    def dump_as_cdf_resource(self) -> dict[str, Any]:
        output = super().dump_as_cdf_resource()
        instances = self.instances()
        for instance_type in ["nodes", "edges"]:
            if resources := getattr(instances, instance_type):
                output[instance_type] = sorted(
                    (
                        remove_read_only_fields(resource.dump_as_cdf_resource(camel_case=False))
                        for resource in resources
                    ),
                    key=self._external_id_key,
                )
        return output

    @classmethod
    def load_from_cdf_resources(cls: TypingType[Self], data: dict[str, Any]) -> Self:
        raise NotImplementedError()


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
