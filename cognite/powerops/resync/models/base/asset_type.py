from __future__ import annotations

import json
from abc import ABC
from collections import Counter, defaultdict
from typing import ClassVar, Optional, TypeVar, Union

from cognite.client.data_classes import Asset, Label, Relationship, TimeSeries
from pydantic import BaseModel
from typing_extensions import Self

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.utils.serialization import get_pydantic_annotation

from .cdf_resources import CDFFile, CDFSequence
from .resource_type import ResourceType


class NonAssetType(BaseModel, ABC, arbitrary_types_allowed=True):
    ...


class AssetType(ResourceType, ABC, arbitrary_types_allowed=True, validate_assignment=True):
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
        for field_name, _field in to.model_fields.items():
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
                    elif self.type_ == "benchmarking_configuration":
                        target_type = "bid_process_configuration_asset"
                    relationships.append(self._create_relationship(target.external_id, "ASSET", target_type))
            elif isinstance(field_value, NonAssetType) or (
                isinstance(field_value, list) and field_value and isinstance(field_value[0], NonAssetType)
            ):
                non_asset_types: list[NonAssetType] = field_value if isinstance(field_value, list) else [field_value]
                for target in non_asset_types:
                    relationships.extend(self._relationships(target))

        return relationships

    def _create_relationship(self, target_external_id: str, target_cdf_type: str, target_type: str) -> Relationship:
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
            annotation, _ = get_pydantic_annotation(field.annotation, type(self))
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
                    metadata[f"{field_name}:{k}"] = str(v)
            elif isinstance(value, list) and value and isinstance(value[0], NonAssetType):
                metadata[field_name] = json.dumps([item.model_dump(exclude_unset=True) for item in value])
            elif isinstance(value, (dict, list)) and value:
                metadata[field_name] = json.dumps(value)
            elif isinstance(value, (dict, list)) and not value:
                continue
            elif field.annotation in (str, int, float):
                metadata[field_name] = str(value)
            elif field.annotation in (Optional[str], Optional[int], Optional[float]):
                metadata[field_name] = str(value) if value is not None else ""
            else:
                raise NotImplementedError(f"Cannot handle metadata of type {field.annotation}")
        return metadata

    @classmethod
    def from_asset(cls, asset: Asset) -> Self:
        instance = cls(name=asset.name, description=asset.description, **cls._load_metadata(asset.metadata))
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


T_Asset_Type = TypeVar("T_Asset_Type", bound=AssetType)
