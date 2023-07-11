from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Optional

from cognite.client.data_classes import Asset, TimeSeries
from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models._base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence


class Generator(AssetType):
    type_: ClassVar[str] = "generator"
    label = AssetLabel.GENERATOR
    p_min: float
    penstock: str
    startcost: float
    start_stop_cost_time_series: Optional[TimeSeries] = None
    generator_efficiency_curve: Optional[CDFSequence] = None
    turbine_efficiency_curve: Optional[CDFSequence] = None


class Reservoir(AssetType):
    type_: ClassVar[str] = "reservoir"
    label = AssetLabel.RESERVOIR
    display_name: str
    ordering: str


class Plant(AssetType):
    type_: ClassVar[str] = "plant"
    label = AssetLabel.PLANT
    display_name: str
    ordering: str
    head_loss_factor: float
    outlet_level: float
    p_min: float
    p_max: float
    penstock_head_loss_factors: dict
    generators: list[Generator] = Field(default_factory=list)
    inlet_reservoir: Optional[Reservoir] = None
    p_min_time_series: Optional[TimeSeries] = None
    p_max_time_series: Optional[TimeSeries] = None
    water_value_time_series: Optional[TimeSeries] = None
    feeding_fee_time_series: Optional[TimeSeries] = None
    outlet_level_time_series: Optional[TimeSeries] = None
    inlet_level_time_series: Optional[TimeSeries] = None
    head_direct_time_series: Optional[TimeSeries] = None


class WaterCourseShop(NonAssetType):
    penalty_limit: str


class Watercourse(AssetType):
    type_: ClassVar[str] = "watercourse"
    label = AssetLabel.WATERCOURSE
    shop: WaterCourseShop
    config_version: str = Field(exclude=True)
    model_file: Path = Field(exclude=True)
    processed_model_file: Path = Field(exclude=True)
    plants: list[Plant]
    production_obligation_time_series: list[TimeSeries] = Field(default_factory=list)


class PriceArea(AssetType):
    type_: ClassVar[str] = "price_area"
    label = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    price_areas: list[PriceArea] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    reservoirs: list[Reservoir] = Field(default_factory=list)
