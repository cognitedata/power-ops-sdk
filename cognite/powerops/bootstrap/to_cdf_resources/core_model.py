from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field, fields
from typing import ClassVar

import pandas as pd
from cognite.client.data_classes import Asset, Label, Relationship, Sequence, SequenceData, TimeSeries

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.bootstrap.to_cdf_resources.create_relationship_types import basic_relationship


@dataclass
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
            if TimeSeries.__name__ in f.type:
                r = basic_relationship(
                    source_type="ASSET",
                    target_type="TIMESERIES",
                    source_external_id=self.external_id,
                    target_external_id=value.external_id,
                    label=RelationshipLabel(f"relationship_to.{f.name}"),
                )
                relationships.append(r)
            elif Sequence.__name__ in f.type:
                r = basic_relationship(
                    source_external_id=self.external_id,
                    source_type="ASSET",
                    target_external_id=value.sequence.external_id,
                    target_type="SEQUENCE",
                    label=RelationshipLabel(f"relationship_to.{f.name}"),
                )
                relationships.append(r)
        return relationships

    def as_asset(self):
        metadata = {}
        for f in fields(self):
            if any(cdf_type in f.type for cdf_type in [CDFSequence.__name__, TimeSeries.__name__]) or f.name == "name":
                continue

            metadata[f.name] = getattr(self, f.name)

        return Asset(
            external_id=self.external_id,
            name=self.name,
            parent_external_id=self.parent_external_id,
            metadata=metadata,
            labels=[Label(self.label.value)],
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
    generators: list[Generator]
    inlet_reservoirs: list[Reservoir]
    p_min_timeseries: TimeSeries
    p_max_timeseries: TimeSeries
    water_value: TimeSeries
    feeding_fee: TimeSeries
    outlet_level_timeseries: TimeSeries
    inlet_level: TimeSeries


@dataclass
class Watercourse(Type):
    type_: ClassVar[str] = "watercourse"
    label = AssetLabel.WATERCOURSE
    shop_penalty_limit: float
    plants: list[Plant]


@dataclass
class PriceArea(Type):
    type_: ClassVar[str] = "price_area"
    label = AssetLabel.PRICE_AREA
    day_ahead_price: TimeSeries
    plants: list[Plant]
    watercourses: list[Watercourse]


@dataclass
class CoreModel:
    price_areas: list[PriceArea] = field(default_factory=list)
    watercourses: list[Watercourse] = field(default_factory=list)
    plants: list[Plant] = field(default_factory=list)
    generators: list[Generator] = field(default_factory=list)
    reservoirs: list[Reservoir] = field(default_factory=list)
