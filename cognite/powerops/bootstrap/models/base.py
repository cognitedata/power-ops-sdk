from __future__ import annotations

import json
from abc import ABC
from typing import ClassVar, Optional, Union

import pandas as pd
from cognite.client.data_classes import Asset, Label, Relationship, Sequence, SequenceData, TimeSeries
from pydantic import BaseModel, ConfigDict

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.bootstrap.data_classes.to_delete import SequenceContent

ROOT_ASSET = Asset(
    external_id="power_ops",
    name="PowerOps",
)


class Type(BaseModel, ABC):
    type_: ClassVar[Optional[str]] = None
    label: ClassVar[AssetLabel]
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)
    name: str
    description: Optional[str] = None
    _external_id: Optional[str] = None
    _parend_external_id: Optional[str] = None

    @property
    def external_id(self) -> str:
        if self._external_id:
            return self._external_id
        return f"{self.type_}_{self.name}"

    @external_id.setter
    def external_id(self, value: str) -> None:
        self._external_id = value

    @property
    def parent_external_id(self) -> str:
        if self._parend_external_id:
            return self._parend_external_id
        return f"{self.type_}s"

    @parent_external_id.setter
    def parent_external_id(self, value: str) -> None:
        self._parend_external_id = value

    @property
    def parent_name(self):
        return self.parent_external_id.replace("_", " ").title()

    def relationships(self) -> list[Relationship]:
        relationships = []
        for field_name, field in self.model_fields.items():
            value = getattr(self, field_name)
            if not value:
                continue
            if isinstance(value, list) and value and isinstance(value[0], Type):
                for target in value:
                    relationships.append(self._create_relationship(target.external_id, "ASSET", target.type_))
            elif isinstance(value, Type):
                target_type = value.type_
                if self.type_ == "plant" and value.type_ == "reservoir":
                    target_type = "inlet_reservoir"
                relationships.append(self._create_relationship(value.external_id, "ASSET", target_type))
            elif any(cdf_type in str(field.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]):
                if TimeSeries.__name__ in str(field.annotation):
                    target_type = "TIMESERIES"
                    target_external_id = value.external_id
                elif CDFSequence.__name__ in str(field.annotation):
                    target_type = "SEQUENCE"
                    target_external_id = value.sequence.external_id
                else:
                    raise ValueError(f"Unexpected type {field.annotation}")
                relationships.append(self._create_relationship(target_external_id, target_type, field_name))
        return relationships

    def as_asset(self):
        metadata = {}
        for field_name, field in self.model_fields.items():
            if any(
                cdf_type in str(field.annotation) for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]
            ) or field_name in {"name", "description", "label", "parent_external_id"}:
                continue
            value = getattr(self, field_name)
            if (
                value is None
                or isinstance(value, Type)
                or (isinstance(value, list) and value and isinstance(value[0], Type))
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
            metadata=metadata if metadata else None,
            description=self.description,
        )

    def sequences(self) -> list[Sequence | SequenceContent]:
        output = []
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if not value:
                continue
            elif isinstance(value, list) and isinstance(value[0], CDFSequence):
                for v in value:
                    output.append(v.sequence)
                    output.append(SequenceContent(sequence_external_id=v.sequence.external_id, data=v.content))
            elif isinstance(value, CDFSequence):
                output.append(value.sequence)
                output.append(SequenceContent(sequence_external_id=value.sequence.external_id, data=value.content))
        return output

    def _create_relationship(
        self,
        target_external_id: str,
        target_cdf_type: str,
        target_type: str,
    ) -> Relationship:
        return Relationship(
            external_id=f"{self.external_id}.{target_external_id}",
            source_external_id=self.external_id,
            source_type="ASSET",
            target_external_id=target_external_id,
            target_type=target_cdf_type,
            labels=[Label(external_id=RelationshipLabel(f"relationship_to.{target_type}").value)],
        )


class CDFSequence(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)
    sequence: Sequence
    content: Union[SequenceData, pd.DataFrame]


class Model(BaseModel, ABC):
    def assets(self) -> list[Asset]:
        return [item.as_asset() for f in self.model_fields for item in getattr(self, f)]

    def relationships(self) -> list[Relationship]:
        return [edge for f in self.model_fields for item in getattr(self, f) for edge in item.relationships()]

    def sequences(self) -> list[Sequence | SequenceContent]:
        return [sequence for f in self.model_fields for item in getattr(self, f) for sequence in item.sequences()]


class NonAssetType(BaseModel, ABC):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)
