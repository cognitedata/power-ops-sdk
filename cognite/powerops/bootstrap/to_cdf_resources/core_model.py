from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import ClassVar

import pandas as pd
from cognite.client.data_classes import Sequence, SequenceData, TimeSeries


@dataclass
class Type(ABC):
    type_: ClassVar[str]
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

    def get_relationships(self):
        raise NotImplementedError

    def as_asset(self):
        raise NotImplementedError()


@dataclass
class CDFSequence:
    sequence: Sequence
    content: SequenceData | pd.DataFrame


@dataclass
class Generator(Type):
    type_: ClassVar[str] = "generator"
    p_min: float
    penstock: int
    start_cost: float
    generator_efficiency_curve: CDFSequence | None = None
    turbine_efficiency_curve: CDFSequence | None = None


@dataclass
class Reservoir(Type):
    type_: ClassVar[str] = "reservoir"
    display_name: str
    ordering: int


@dataclass
class Plant(Type):
    type_: ClassVar[str] = "plant"
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
    shop_penalty_limit: float
    plants: list[Plant]


@dataclass
class PriceArea(Type):
    type_: ClassVar[str] = "price_area"
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
