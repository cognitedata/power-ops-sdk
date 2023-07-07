from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass, field, fields
from typing import ClassVar

import pandas as pd
from cognite.client.data_classes import Asset, Label, Relationship, Sequence, SequenceData, TimeSeries

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.bootstrap.to_cdf_resources.create_relationship_types import basic_relationship


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
                    r = basic_relationship(
                        source_external_id=self.external_id,
                        source_type="ASSET",
                        target_type="ASSET",
                        target_external_id=target.external_id,
                        label=RelationshipLabel(f"relationship_to.{target.type_}"),
                    )
                    relationships.append(r)
            elif isinstance(value, Type):
                label_type = value.type_
                if self.type_ == "plant" and value.type_ == "reservoir":
                    label_type = "inlet_reservoir"

                r = basic_relationship(
                    source_external_id=self.external_id,
                    source_type="ASSET",
                    target_type="ASSET",
                    target_external_id=value.external_id,
                    label=RelationshipLabel(f"relationship_to.{label_type}"),
                )
                relationships.append(r)
            elif any(cdf_type in f.type for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]):
                if TimeSeries.__name__ in f.type:
                    target_type = "TIMESERIES"
                    target_external_id = value.external_id
                elif CDFSequence.__name__ in f.type:
                    target_type = "SEQUENCE"
                    target_external_id = value.sequence.external_id
                else:
                    raise ValueError(f"Unexpected type {f.type}")
                r = basic_relationship(
                    source_external_id=self.external_id,
                    source_type="ASSET",
                    target_external_id=target_external_id,
                    target_type=target_type,
                    label=RelationshipLabel(f"relationship_to.{f.name}"),
                )
                relationships.append(r)
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


@dataclass
class CDFSequence:
    sequence: Sequence
    content: SequenceData | pd.DataFrame


@dataclass
class Generator(Type):
    type_: ClassVar[str] = "generator"
    label = AssetLabel.GENERATOR
    p_min: float
    penstock: str
    startcost: float
    start_stop_cost_time_series: TimeSeries | None = None
    generator_efficiency_curve: CDFSequence | None = None
    turbine_efficiency_curve: CDFSequence | None = None


@dataclass
class Reservoir(Type):
    type_: ClassVar[str] = "reservoir"
    label = AssetLabel.RESERVOIR
    display_name: str
    ordering: int


@dataclass
class Plant(Type):
    type_: ClassVar[str] = "plant"
    label = AssetLabel.PLANT
    display_name: str
    ordering: int
    head_loss_factor: float
    outlet_level: float
    p_min: float
    p_max: float
    penstock_head_loss_factors: dict
    generators: list[Generator] = field(default_factory=list)
    inlet_reservoir: Reservoir | None = None
    p_min_time_series: TimeSeries | None = None
    p_max_time_series: TimeSeries | None = None
    water_value_time_series: TimeSeries | None = None
    feeding_fee_time_series: TimeSeries | None = None
    outlet_level_time_series: TimeSeries | None = None
    inlet_level_time_series: TimeSeries | None = None
    head_direct_time_series: TimeSeries | None = None


@dataclass
class Watercourse(Type):
    type_: ClassVar[str] = "watercourse"
    label = AssetLabel.WATERCOURSE
    shop_penalty_limit: float
    plants: list[Plant]
    production_obligation_time_series: list[TimeSeries] = field(default_factory=list)


@dataclass
class PriceArea(Type):
    type_: ClassVar[str] = "price_area"
    label = AssetLabel.PRICE_AREA
    day_ahead_price: TimeSeries | None = None
    plants: list[Plant] = field(default_factory=list)
    watercourses: list[Watercourse] = field(default_factory=list)


@dataclass
class CoreModel:
    price_areas: list[PriceArea] = field(default_factory=list)
    watercourses: list[Watercourse] = field(default_factory=list)
    plants: list[Plant] = field(default_factory=list)
    generators: list[Generator] = field(default_factory=list)
    reservoirs: list[Reservoir] = field(default_factory=list)

    def as_assets(self) -> list[Asset]:
        return [item.as_asset() for f in fields(self) for item in getattr(self, f.name)]

    def as_relationships(self) -> list[Relationship]:
        return [edge for f in fields(self) for item in getattr(self, f.name) for edge in item.get_relationships()]
