from __future__ import annotations
from collections import defaultdict

import json
from abc import ABC
from deepdiff import DeepDiff
from deepdiff.model import PrettyOrderedSet

from pathlib import Path
from typing import Any, ClassVar, Iterable, Optional, Union, TypeVar, get_args
from typing import Type as TypingType
from pydantic import BaseModel, ConfigDict

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Label, Relationship, TimeSeries
from cognite.client.data_classes.data_modeling.instances import EdgeApply, NodeApply, InstancesApply

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.clients.data_classes._core import DomainModelApply
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence
from cognite.powerops.resync.models.helpers import (
    format_change_binary,
    format_value_added,
    format_value_removed,
    isinstance_list,
    match_field_from_relationship,
    pydantic_model_class_candidate,
)

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
        relationships = []
        for field_name, field in self.model_fields.items():
            value = getattr(self, field_name)
            if not value:
                continue
            if isinstance_list(value, AssetType):
                relationships.extend(
                    self._create_relationship(target.external_id, "ASSET", target.type_) for target in value
                )
            elif isinstance(value, AssetType):
                target_type = value.type_
                if self.type_ == "plant" and value.type_ == "reservoir":
                    target_type = "inlet_reservoir"
                relationships.append(self._create_relationship(value.external_id, "ASSET", target_type))
            elif any(
                cdf_type in str(field.annotation)
                for cdf_type in [
                    CDFSequence.__name__,
                    TimeSeries.__name__,
                ]
            ):
                if TimeSeries.__name__ in str(field.annotation):
                    target_type = "TIMESERIES"
                elif CDFSequence.__name__ in str(field.annotation):
                    target_type = "SEQUENCE"
                else:
                    raise ValueError(f"Unexpected type {field.annotation}")

                if isinstance(value, list):
                    relationships.extend(
                        self._create_relationship(
                            target.external_id,
                            target_type,
                            field_name,
                        )
                        for target in value
                    )
                else:
                    relationships.append(
                        self._create_relationship(
                            value.external_id,
                            target_type,
                            field_name,
                        )
                    )
        return relationships

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

    def _create_relationship(
        self,
        target_external_id: str,
        target_cdf_type: str,
        target_type: str,
    ) -> Relationship:
        source_type = "ASSET"

        # The market model uses the suffix CDF type for the relationship label, while the core model does not.
        try:
            # Core Model
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

    @classmethod
    def _parse_asset_metadata(cls, metadata: dict[str, Any] = None) -> dict[str, Any]:
        raise NotImplementedError()

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
            limit=-1,
        )
        for r in relationships:
            field = match_field_from_relationship(cls.model_fields.keys(), r)
            target_type = r.target_type.lower()
            relationship_target = None

            if target_type == "asset":
                if r.target_external_id in AssetType._instantiated_assets:
                    relationship_target = AssetType._instantiated_assets[r.target_external_id]

                else:
                    target_class = [y for x, y in cls._field_name_asset_resource_class() if field == x and y][0]
                    relationship_target = target_class.from_cdf(
                        client=client,
                        external_id=r.target_external_id,
                        fetch_metadata=fetch_metadata,
                        fetch_content=fetch_content,
                    )

            elif target_type == "timeseries":
                relationship_target = client.time_series.retrieve(external_id=r.target_external_id)
            elif target_type == "sequence":
                relationship_target = CDFSequence.from_cdf(client, r.target_external_id, fetch_content)
            elif target_type == "file":
                relationship_target = CDFFile.from_cdf(client, r.target_external_id, fetch_content)
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
                asset = client.assets.retrieve(external_id=external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")

        # Prepare non-asset metadata fields
        additional_fields = {
            field: [] if "list" in str(field_info.annotation) else None
            for field, field_info in cls.model_fields.items()
            if field in [x for x, _ in cls._field_name_asset_resource_class()]
            or any(cdf_type in str(field_info.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__])
        }

        # Populate non-asset metadata fields according to relationships/flags
        # `Additional_fields` is modified in-place by `_fetch_metadata`
        if fetch_metadata:
            cls._fetch_and_set_metadata(
                client,
                additional_fields,
                asset.external_id,
                fetch_metadata,
                fetch_content,
            )

        return cls._from_asset(asset, additional_fields)

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
        return summary


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
        """AssetType fields are are of type list[AssetType] or Optional[AssetType]"""
        # todo? method is identical to _field_name_asset_resource_class in AssetType
        # * unsure how to reuse?
        for field_name in cls.model_fields:
            class_ = cls.model_fields[field_name].annotation
            if pydantic_model_class_candidate(class_):
                asset_resource_class = get_args(class_)[0]
                if issubclass(asset_resource_class, AssetType):
                    yield field_name, asset_resource_class

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary = super().summary()
        summary[self.model_name]["cdf"]["assets"] = len(self.assets())
        summary[self.model_name]["cdf"]["relationships"] = len(self.relationships())
        summary[self.model_name]["cdf"]["parent_assets"] = len(self.parent_assets())
        return summary

    @classmethod
    def from_cdf(
        cls: TypingType[T_Asset_Model],
        client: CogniteClient,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> T_Asset_Model:
        if fetch_content and not fetch_metadata:
            raise ValueError("Cannot fetch content without also fetching metadata")

        output = defaultdict(list)

        for field_name, asset_cls in cls._field_name_asset_resource_class():
            assets = client.assets.retrieve_subtree(external_id=asset_cls.parent_external_id)
            for asset in assets:
                if asset.external_id == asset_cls.parent_external_id:
                    continue
                instance = asset_cls.from_cdf(
                    client=client,
                    asset=asset,
                    fetch_metadata=fetch_metadata,
                    fetch_content=fetch_content,
                )
                output[field_name].append(instance)

        return cls(**output)

    def _prepare_for_diff(self: T_Asset_Model) -> dict[str:dict]:
        raise NotImplementedError()

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

        return InstancesApply(nodes=list(nodes.values()), edges=list(edges.values()))

    def _domain_models(self) -> Iterable[DomainModelApply]:
        for field_name in self.model_fields:
            items = getattr(self, field_name)
            if isinstance(items, list) and items and isinstance(items[0], DomainModelApply):
                yield from items
            if isinstance(items, DomainModelApply):
                yield items
            if isinstance(items, dict) and items and isinstance(next(iter(items.values())), DomainModelApply):
                yield from items.values()

    def summary(self) -> dict[str, dict[str, dict[str, int]]]:
        summary = super().summary()
        instances = self.instances()
        summary[self.model_name]["cdf"]["nodes"] = len(instances.nodes)
        summary[self.model_name]["cdf"]["edges"] = len(instances.edges)
        return summary


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
