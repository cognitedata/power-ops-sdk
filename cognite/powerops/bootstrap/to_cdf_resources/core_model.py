from abc import ABC
from dataclasses import dataclass
from typing import ClassVar

from cognite.client.data_classes import Sequence, SequenceData, TimeSeries


@dataclass
class Type(ABC):
    type_: ClassVar[str]
    name: str
    external_id: str

    def get_relationships(self):
        raise NotImplementedError

    def as_asset(self):
        raise NotImplementedError()


@dataclass
class CDFSequence:
    sequence: Sequence
    content: SequenceData


@dataclass
class Generator(Type):
    type_: ClassVar[str] = "generator"
    p_min: float
    penstock: int
    start_cost: float
    generator_efficiency_curve: CDFSequence
    turbine_efficiency_curve: CDFSequence


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
    price_areas: list[PriceArea]
    watercourses: list[Watercourse]
    plants: list[Plant]
    generators: list[Generator]
    reservoirs: list[Reservoir]
