from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass, fields
from typing import ClassVar

import pandas as pd
from cognite.client.data_classes import Asset, Label, Relationship, Sequence, SequenceData, TimeSeries

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel


@dataclass()
class Type(ABC):
    type_: ClassVar[str]
    label: ClassVar[AssetLabel]
    name: str

    @property
    def external_id(self) -> str:
        return f"{self.type_}_{self.name}"

    @property
    def parent_external_id(self):
        return f"{self.type_}s"

    @property
    def parent_name(self):
        return self.parent_external_id.replace("_", " ").title()

    def get_relationships(self) -> list[Relationship]:
        relationships = []
        for f in fields(self):
            value = getattr(self, f.name)
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
            elif any(cdf_type in f.type for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]):
                if TimeSeries.__name__ in f.type:
                    target_type = "TIMESERIES"
                    target_external_id = value.external_id
                elif CDFSequence.__name__ in f.type:
                    target_type = "SEQUENCE"
                    target_external_id = value.sequence.external_id
                else:
                    raise ValueError(f"Unexpected type {f.type}")
                relationships.append(self._create_relationship(target_external_id, target_type, f.name))
        return relationships

    def as_asset(self):
        metadata = {}
        for f in fields(self):
            if any(cdf_type in f.type for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]) or f.name == "name":
                continue
            value = getattr(self, f.name)
            if value is None or isinstance(value, Type) or (isinstance(value, list) and isinstance(value[0], Type)):
                continue
            if isinstance(value, dict):
                value = json.dumps(value)
            metadata[f.name] = value

        return Asset(
            external_id=self.external_id,
            name=self.name,
            parent_external_id=self.parent_external_id,
            labels=[Label(self.label.value)],
            metadata=metadata if metadata else None,
        )

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


@dataclass
class CDFSequence:
    sequence: Sequence
    content: SequenceData | pd.DataFrame


@dataclass
class Model(ABC):
    def as_assets(self) -> list[Asset]:
        return [item.as_asset() for f in fields(self) for item in getattr(self, f.name)]

    def as_relationships(self) -> list[Relationship]:
        return [edge for f in fields(self) for item in getattr(self, f.name) for edge in item.get_relationships()]
