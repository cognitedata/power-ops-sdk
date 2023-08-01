from __future__ import annotations
import json

from pathlib import Path
from typing import Any, ClassVar, Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.helpers import isinstance_list, match_field_from_relationship


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
            "penstock_head_loss_factors": penstock_head_loss_factors
        }

    @classmethod
    def _from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> Plant:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "generators": [],
            "inlet_reservoir": None,
            "p_min_time_series": None,
            "p_max_time_series": None,
            "water_value_time_series": None,
            "feeding_fee_time_series": None,
            "outlet_level_time_series": None,
            "inlet_level_time_series": None,
            "head_direct_time_series": None,
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    # todo: handle later -- we only want to instantiate a class
                    # one per ext id. Probably a dict when re-written to high-level.
                    # finding the field is still its own challenge
                    cdf_fields[field] = None if field == "inlet_reservoir" else []

                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(external_id=r.target_external_id)
        return cls._from_asset(asset, cdf_fields)


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
            "shop": WaterCourseShop(penalty_limit=asset_metadata.get("shop:penalty_limit", "")),
            "model_file": None,
            "processed_model_file": None,
        }

    @classmethod
    def _from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> Watercourse:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "config_version": None,
            "plants": [],
            "production_obligation_time_series": [],
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    cdf_fields[field] = []
                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(external_id=r.target_external_id)

        return cls._from_asset(asset, cdf_fields)


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    
    @classmethod
    def _parse_asset_metadata(cls, asset_metadata: dict[str, str]) -> dict[str, Any]:
        # In order to maintain the AssetType structure
        return {}
        

    @classmethod
    def _from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> PriceArea:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "dayahead_price_time_series": None,
            "plants": [],
            "watercourses": [],
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    cdf_fields[field] = []
                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(external_id=r.target_external_id)

        return cls._from_asset(asset, cdf_fields)


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    reservoirs: list[Reservoir] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)

    def _prepare_for_diff(self: ProductionModel) -> dict:
        clone = self.model_copy(deep=True)

        for model_field in clone.model_fields:
            field_value = getattr(clone, model_field)
            if isinstance_list(field_value, AssetType):
            # if isinstance(field_value, list) and field_value and isinstance(field_value[0], AssetType):
                # Sort the asset types to have comparable order for diff
                _sorted = sorted(field_value, key=lambda x: x.external_id)
                # Prepare each asset type for diff
                _prepared = map(lambda x: x._asset_type_prepare_for_diff(), _sorted)
                setattr(clone, model_field, list(_prepared))
            elif isinstance(field_value, AssetType):
                # does not apply to this model, but
                # might be used in a higher level of abstraction
                field_value._asset_type_prepare_for_diff()
        return clone.model_dump()
