from __future__ import annotations
import json

from pathlib import Path
from typing import Any, ClassVar, Optional, Union

from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.helpers import isinstance_list


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
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        return {
            "p_min": float(asset_metadata.get("p_min", 0.0)),
            "penstock": asset_metadata.get("penstock", ""),
            "startcost": float(asset_metadata.get("startcost", 0.0)),
        }


class Reservoir(AssetType):
    parent_external_id: ClassVar[str] = "reservoirs"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str
    ordering: str

    @classmethod
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        return {
            "display_name": asset_metadata.get("display_name", ""),
            "ordering": asset_metadata.get("ordering", ""),
        }


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
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        penstock_head_loss_factors_raw: str = asset_metadata.get("penstock_head_loss_factors", "")
        try:
            penstock_head_loss_factors = json.loads(penstock_head_loss_factors_raw)
            if not isinstance(penstock_head_loss_factors, dict):
                raise TypeError
        except (json.JSONDecodeError, TypeError):
            penstock_head_loss_factors = {}

        return {
            "display_name": asset_metadata.get("display_name", ""),
            "ordering": asset_metadata.get("ordering", ""),
            "head_loss_factor": float(asset_metadata.get("head_loss_factor", 0.0)),
            "outlet_level": float(asset_metadata.get("outlet_level", 0.0)),
            "p_min": float(asset_metadata.get("p_min", 0.0)),
            "p_max": float(asset_metadata.get("p_max", 0.0)),
            "penstock_head_loss_factors": penstock_head_loss_factors,
        }


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
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        return {
            "config_version": "",
            "shop": WaterCourseShop(penalty_limit=asset_metadata.get("shop:penalty_limit", "")),
            "model_file": None,
            "processed_model_file": None,
        }


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    @classmethod
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        # Maintain the AssetType structure
        return {}


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    reservoirs: list[Reservoir] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)

    def _prepare_for_diff(self: ProductionModel) -> dict:
        clone = self.model_copy(deep=True)

        for model_field in clone.model_fields:
            field_value = getattr(clone, model_field)
            if isinstance_list(field_value, AssetType):
                # Sort the asset types to have comparable order for diff
                _sorted = sorted(field_value, key=lambda x: x.external_id)
                # Prepare each asset type for diff
                _prepared = map(lambda x: x._asset_type_prepare_for_diff(), _sorted)
                setattr(clone, model_field, list(_prepared))
            elif isinstance(field_value, AssetType):
                field_value._asset_type_prepare_for_diff()
        # Some fields are have been set to their external_id which gives a warning we can ignore
        return clone.model_dump(warnings=False)
