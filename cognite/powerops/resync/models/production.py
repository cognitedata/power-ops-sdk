from __future__ import annotations
import json

from pathlib import Path
from typing import ClassVar, Optional, Union

from cognite.client import CogniteClient
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
    def _from_asset(
        cls,
        asset: Asset,
        start_stop_cost_time_series: Optional[TimeSeries] = None,
        generator_efficiency_curve: Optional[CDFSequence] = None,
        turbine_efficiency_curve: Optional[CDFSequence] = None,
    ) -> Generator:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            p_min=float(asset.metadata.get("p_min", 0.0)),
            penstock=asset.metadata.get("penstock", ""),
            startcost=float(asset.metadata.get("startcost", 0.0)),
            start_stop_cost_time_series=start_stop_cost_time_series,
            generator_efficiency_curve=generator_efficiency_curve,
            turbine_efficiency_curve=turbine_efficiency_curve,
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = False,
        fetch_content: bool = False,
    ) -> Generator:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf = {
                "start_stop_cost_time_series": None,
                "generator_efficiency_curve": None,
                "turbine_efficiency_curve": None,
            }
        
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                limit=-1,
                target_types=["TIMESERIES", "SEQUENCE"],
            )
            for r in relationships:
                resource_ext_id = r.target_external_id
                field =  list(filter(lambda k: k in r.target_external_id, cls.model_fields.keys()))[0]
                if r.target_type.lower() == "sequence":
                    cdf[field] = CDFSequence.from_cdf(client, resource_ext_id, fetch_content)
                if r.target_type.lower() == "tierieseries":
                    cdf[field] = client.time_series.retrieve(resource_ext_id)
        
        return cls._from_asset(asset, **cdf)
            



class Reservoir(AssetType):
    parent_external_id: ClassVar[str] = "reservoirs"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str
    ordering: str

    @classmethod
    def _from_asset(cls, asset: Asset) -> Reservoir:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            display_name=asset.metadata.get("display_name", ""),
            ordering=asset.metadata.get("ordering", ""),
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = False,
        fetch_content: bool = False,
    ) -> Reservoir:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if asset:
            return cls._from_asset(asset)
        if asset := client.assets.retrieve(external_id):
            return cls._from_asset(asset)
        raise ValueError(f"Could not retrieve asset with {external_id=}")


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

    # @classmethod
    # def from_asset(cls, asset: Asset) -> Plant:
    #     penstock_head_loss_factors_raw: str = asset.metadata.get("penstock_head_loss_factors", "")
    #     try:
    #         penstock_head_loss_factors = json.loads(penstock_head_loss_factors_raw)
    #         if not isinstance(penstock_head_loss_factors, dict):
    #             raise TypeError
    #     except (json.JSONDecodeError, TypeError):
    #         penstock_head_loss_factors = {}
    #     return cls(
    #         _external_id=asset.external_id,
    #         name=asset.name,
    #         description=asset.description,
    #         display_name=asset.metadata.get("display_name", ""),
    #         ordering=asset.metadata.get("ordering", ""),
    #         head_loss_factor=float(asset.metadata.get("head_loss_factor", 0.0)),
    #         outlet_level=float(asset.metadata.get("outlet_level", 0.0)),
    #         p_min=float(asset.metadata.get("p_min", 0.0)),
    #         p_max=float(asset.metadata.get("p_max", 0.0)),
    #         penstock_head_loss_factors=penstock_head_loss_factors,
    #         generators=[],
    #         inlet_reservoir=None,
    #         p_min_time_series=None,
    #         p_max_time_series=None,
    #         water_value_time_series=None,
    #         feeding_fee_time_series=None,
    #         outlet_level_time_series=None,
    #         inlet_level_time_series=None,
    #         head_direct_time_series=None,
    #     )


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

    # @classmethod
    # def from_asset(cls, asset: Asset) -> Watercourse:
    #     return cls(
    #         _external_id=asset.external_id,
    #         name=asset.name,
    #         description=asset.description,
    #         shop=WaterCourseShop(penalty_limit=asset.metadata.get("penalty_limit", "")),
    #         config_version=None,
    #         model_file=None,
    #         processed_model_file=None,
    #         plants=[],
    #         production_obligation_time_series=[],
    #     )


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    # @classmethod
    # def from_asset(cls, asset: Asset) -> PriceArea:
    #     return cls(
    #         _external_id=asset.external_id,
    #         name=asset.name,
    #         description=asset.description,
    #         dayahead_price_time_series=None,
    #         plants=[],
    #         watercourses=[],
    #     )


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    reservoirs: list[Reservoir] = Field(default_factory=list)
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)

    def _prepare_for_diff(self: ProductionModel) -> dict:
        clone = self.model_copy(deep=True)

        for model_field in clone.model_fields:
            field_value = getattr(clone, model_field)
            if isinstance(field_value, list) and field_value and isinstance(field_value[0], AssetType):
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
