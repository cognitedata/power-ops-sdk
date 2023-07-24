from __future__ import annotations
import json

from pathlib import Path
from typing import ClassVar, Optional, Union

from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence


class Generator(AssetType):
    parent_external_id: ClassVar[str] = "generators"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.GENERATOR
    p_min: float
    penstock: str
    startcost: float
    start_stop_cost_time_series: Optional[TimeSeries] = None
    generator_efficiency_curve: Optional[CDFSequence] = None
    turbine_efficiency_curve: Optional[CDFSequence] = None

    @classmethod
    def from_asset(cls, asset: Asset) -> Generator:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            p_min=float(asset.metadata.get("p_min", 0.0)),
            penstock=asset.metadata.get("penstock", ""),
            startcost=float(asset.metadata.get("startcost", 0.0)),
            start_stop_cost_time_series=None,
            generator_efficiency_curve=None,
            turbine_efficiency_curve=None,
        )


class Reservoir(AssetType):
    parent_external_id: ClassVar[str] = "reservoirs"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str
    ordering: str

    @classmethod
    def from_asset(cls, asset: Asset) -> Reservoir:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            display_name=asset.metadata.get("display_name", ""),
            ordering=asset.metadata.get("ordering", ""),
        )


class Plant(AssetType):
    parent_external_id: ClassVar[str] = "plants"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PLANT
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

    @classmethod
    def from_asset(cls, asset: Asset) -> Plant:
        penstock_head_loss_factors_raw: str = asset.metadata.get("penstock_head_loss_factors", "")
        try:
            penstock_head_loss_factors = json.loads(penstock_head_loss_factors_raw)
            if not isinstance(penstock_head_loss_factors, dict):
                raise TypeError
        except (json.JSONDecodeError, TypeError):
            penstock_head_loss_factors = {}
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            display_name=asset.metadata.get("display_name", ""),
            ordering=asset.metadata.get("ordering", ""),
            head_loss_factor=float(asset.metadata.get("head_loss_factor", 0.0)),
            outlet_level=float(asset.metadata.get("outlet_level", 0.0)),
            p_min=float(asset.metadata.get("p_min", 0.0)),
            p_max=float(asset.metadata.get("p_max", 0.0)),
            penstock_head_loss_factors=penstock_head_loss_factors,
            generators=[],
            inlet_reservoir=None,
            p_min_time_series=None,
            p_max_time_series=None,
            water_value_time_series=None,
            feeding_fee_time_series=None,
            outlet_level_time_series=None,
            inlet_level_time_series=None,
            head_direct_time_series=None,
        )


class WaterCourseShop(NonAssetType):
    penalty_limit: str


class Watercourse(AssetType):
    model_config: ClassVar[ConfigDict] = ConfigDict(protected_namespaces=tuple())
    parent_external_id: ClassVar[str] = "watercourses"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.WATERCOURSE
    shop: WaterCourseShop
    config_version: Optional[str] = Field(exclude=True)
    model_file: Optional[Path] = Field(exclude=True)
    processed_model_file: Optional[Path] = Field(exclude=True)
    plants: list[Plant]
    production_obligation_time_series: list[TimeSeries] = Field(default_factory=list)

    @classmethod
    def from_asset(cls, asset: Asset) -> Watercourse:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            shop=WaterCourseShop(penalty_limit=asset.metadata.get("penalty_limit", "")),
            config_version=None,
            model_file=None,
            processed_model_file=None,
            plants=[],
            production_obligation_time_series=[],
        )


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    @classmethod
    def from_asset(cls, asset: Asset) -> PriceArea:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            dayahead_price_time_series=None,
            plants=[],
            watercourses=[],
        )


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    reservoirs: list[Reservoir] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
