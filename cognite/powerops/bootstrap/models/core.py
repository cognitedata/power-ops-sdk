from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from cognite.client.data_classes import TimeSeries

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel
from cognite.powerops.bootstrap.models.base import CDFSequence, Model, Type


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
    shop_penalty_limit: str
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
class CoreModel(Model):
    price_areas: list[PriceArea] = field(default_factory=list)
    watercourses: list[Watercourse] = field(default_factory=list)
    plants: list[Plant] = field(default_factory=list)
    generators: list[Generator] = field(default_factory=list)
    reservoirs: list[Reservoir] = field(default_factory=list)
