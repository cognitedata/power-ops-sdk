from __future__ import annotations
from collections import defaultdict

import json
from abc import ABC
from typing import ClassVar, Iterable, Optional, Union, TypeVar, get_args
from typing import Type as TypingType
from types import GenericAlias

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Label, Relationship, TimeSeries
from cognite.client.data_classes.data_modeling.instances import EdgeApply, NodeApply
from pydantic import BaseModel, ConfigDict

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.clients.data_classes._core import DomainModelApply, InstancesApply
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence


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
            elif isinstance(value, list) and isinstance(value[0], type_):
                output.extend(value)
            elif isinstance(value, type_):
                output.append(value)
        return output


class AssetType(ResourceType, ABC):
    parent_external_id: ClassVar[str]
    label: ClassVar[Union[AssetLabel, str]]
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)
    parent_description: ClassVar[Optional[str]] = None
    name: str
    description: Optional[str] = None
    _external_id: Optional[str] = None
    _type: Optional[str] = None

    @property
    def external_id(self) -> str:
        if self._external_id:
            return self._external_id
        return f"{self.type_}_{self.name}"

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
        if self._type:
            return self._type
        return self.parent_external_id.removesuffix("s")

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
            if isinstance(value, list) and value and isinstance(value[0], AssetType):
                for target in value:
                    relationships.append(self._create_relationship(target.external_id, "ASSET", target.type_))
            elif isinstance(value, AssetType):
                target_type = value.type_
                if self.type_ == "plant" and value.type_ == "reservoir":
                    target_type = "inlet_reservoir"
                relationships.append(self._create_relationship(value.external_id, "ASSET", target_type))
            elif any(cdf_type in str(field.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]):
                if TimeSeries.__name__ in str(field.annotation):
                    target_type = "TIMESERIES"
                elif CDFSequence.__name__ in str(field.annotation):
                    target_type = "SEQUENCE"
                else:
                    raise ValueError(f"Unexpected type {field.annotation}")

                if isinstance(value, list):
                    for target in value:
                        relationships.append(self._create_relationship(target.external_id, target_type, field_name))
                else:
                    relationships.append(self._create_relationship(value.external_id, target_type, field_name))
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
            if (
                value is None
                or isinstance(value, AssetType)
                or (isinstance(value, list) and value and isinstance(value[0], AssetType))
            ):
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
    def from_asset(cls: TypingType["T_Asset_Type"], asset: Asset) -> "T_Asset_Type":
        "Not yet implemented: CDF relationships"
        raise NotImplementedError()


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
            elif isinstance(value, list) and value and isinstance(value[0], type_):
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
    def _asset_types_and_field_names(cls) -> Iterable[tuple[str, TypingType[AssetType]]]:
        for field_name in cls.model_fields:
            class_ = cls.model_fields[field_name].annotation
            if isinstance(class_, GenericAlias):
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
    def from_cdf(cls: TypingType[T_Asset_Model], client: CogniteClient) -> T_Asset_Model:
        output = defaultdict(list)
        for field_name, asset_cls in cls._asset_types_and_field_names():
            assets = client.assets.retrieve_subtree(external_id=asset_cls.parent_external_id)
            for asset in assets:
                if asset.external_id == asset_cls.parent_external_id:
                    continue
                instance = asset_cls.from_asset(asset)
                output[field_name].append(instance)
        return cls(**output)

    def difference(self: T_Asset_Model, other: T_Asset_Model) -> dict:
        raise NotImplementedError()


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
