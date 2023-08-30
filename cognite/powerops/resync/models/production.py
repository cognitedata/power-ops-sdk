from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Optional, Union, Any

from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field, field_validator, field_serializer

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence

from cognite.powerops.resync.utils.serializer import try_load_dict, parse_time_series


class Generator(AssetType):
    parent_external_id: ClassVar[str] = "generators"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.GENERATOR
    p_min: float = 0.0
    penstock: str = ""
    startcost: float = 0.0
    start_stop_cost_time_series: Optional[TimeSeries] = None
    is_available_time_series: Optional[TimeSeries] = None
    generator_efficiency_curve: Optional[CDFSequence] = None
    turbine_efficiency_curve: Optional[CDFSequence] = None

    @field_serializer("start_stop_cost_time_series", "is_available_time_series")
    def ser_time_series(self, value) -> dict[str, Any]:
        if value is None:
            return {}
        return {"externalId": value.external_id}

    @field_validator("generator_efficiency_curve", "turbine_efficiency_curve", mode="before")
    def parse_sequences(cls, value):
        if value == {}:
            return None
        return value

    @field_validator("start_stop_cost_time_series", "is_available_time_series", mode="before")
    def parse_timeseries(cls, value):
        return parse_time_series(value)


class Reservoir(AssetType):
    parent_external_id: ClassVar[str] = "reservoirs"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str = ""
    ordering: str = ""


class Plant(AssetType):
    parent_external_id: ClassVar[str] = "plants"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PLANT
    display_name: str = ""
    ordering: str = ""
    head_loss_factor: float = 0.0
    outlet_level: float = 0.0
    p_min: float = 0.0
    p_max: float = 0.0
    penstock_head_loss_factors: dict = Field(default_factory=dict)
    generators: list[Generator] = Field(default_factory=list)
    inlet_reservoir: Optional[Reservoir] = None
    p_min_time_series: Optional[TimeSeries] = None
    p_max_time_series: Optional[TimeSeries] = None
    water_value_time_series: Optional[TimeSeries] = None
    feeding_fee_time_series: Optional[TimeSeries] = None
    outlet_level_time_series: Optional[TimeSeries] = None
    inlet_level_time_series: Optional[TimeSeries] = None
    head_direct_time_series: Optional[TimeSeries] = None

    @field_validator("penstock_head_loss_factors", mode="before")
    def parse_str(cls, value) -> dict:
        return try_load_dict(value)

    @field_serializer(
        "p_min_time_series",
        "p_max_time_series",
        "water_value_time_series",
        "feeding_fee_time_series",
        "outlet_level_time_series",
        "inlet_level_time_series",
        "head_direct_time_series",
    )
    def ser_time_series(self, value) -> dict[str, Any]:
        if value is None:
            return {}
        return {"externalId": value.external_id}

    @field_validator(
        "p_min_time_series",
        "p_max_time_series",
        "water_value_time_series",
        "feeding_fee_time_series",
        "outlet_level_time_series",
        "inlet_level_time_series",
        "head_direct_time_series",
        mode="before",
    )
    def parse_timeseries(cls, value):
        return parse_time_series(value)


class WaterCourseShop(NonAssetType):
    penalty_limit: str


class Watercourse(AssetType):
    model_config: ClassVar[ConfigDict] = ConfigDict(protected_namespaces=tuple())
    parent_external_id: ClassVar[str] = "watercourses"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.WATERCOURSE
    shop: WaterCourseShop
    config_version: Optional[str] = Field("", exclude=True)
    model_file: Optional[Path] = Field(None, exclude=True)
    processed_model_file: Optional[Path] = Field(None, exclude=True)
    plants: list[Plant] = Field(default_factory=list)
    production_obligation_time_series: list[TimeSeries] = Field(default_factory=list)

    @field_validator("production_obligation_time_series", mode="before")
    def none_to_empty_list(cls, value) -> list[TimeSeries]:
        if value is None or (isinstance(value, list) and value and value[0] is None):
            return []
        return value

    @field_serializer("production_obligation_time_series")
    def ser_time_series(self, value) -> dict[str, Any]:
        if value is None:
            return []
        return [{"externalId": ts.external_id} for ts in value]


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    @field_serializer("dayahead_price_time_series")
    def ser_time_series(self, value) -> dict[str, Any]:
        if value is None:
            return {}
        return {"externalId": value.external_id}


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    reservoirs: list[Reservoir] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)
