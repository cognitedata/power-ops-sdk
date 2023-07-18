from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Optional

from cognite.client import CogniteClient
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

    @classmethod
    def from_cdf(cls: Watercourse, client:CogniteClient, root_watercourse_asset: Asset,) -> list[Watercourse]:
        watercourses = []
        asset_subtree = client.assets.retrieve_subtree(
            external_id=root_watercourse_asset.external_id, 
        )
        print(asset_subtree)
        for watercourse_asset in asset_subtree:
            if watercourse_asset.external_id == root_watercourse_asset.external_id:
                continue # skip root asset
            watercourses.append(
                cls(
                    external_id=watercourse_asset.external_id,
                    name=watercourse_asset.name,
                    description=watercourse_asset.description,
                    shop = WaterCourseShop(penalty_limit=watercourse_asset.metadata.get("penalty_limit", "")),
                    # Are these storied in cdf at all?
                    config_version=watercourse_asset.metadata.get("config_version", ""),
                    model_file=watercourse_asset.metadata.get("model_file", ""),
                    processed_model_file=watercourse_asset.metadata.get("processed_model_file", ""),
                    plants=[],
                    production_obligation_time_series = [],
            ))

        return watercourses
        


        
        


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

    
    @classmethod
    def from_cdf(cls: ProductionModel, client: CogniteClient) -> ProductionModel:
        model = cls()
        root_asset = client.assets.retrieve(external_id=cls.root_asset.external_id)
        asset_subtree = client.assets.retrieve_subtree(external_id=cls.root_asset.external_id, depth=1)
        for asset in asset_subtree:
            if asset.external_id == "watercourses":
                model.watercourses = [Watercourse.from_cdf(client, asset)]

        # start by downloading all the resources from CDF
        # for resource_type in [
        #     "assets",
        #     "relationships",
        #     "labels",
        #     "events",
        # ]:
        #     # get the api for the resource type
        #     api = getattr(client, resource_type)
        #     # get all the resources from CDF
        #     resources = api.list(data_set_ids=[ds_id], limit=None)
        #     # add the resources to the bootstrap resource collection
        #     model.resources[resource_type].extend(resources)
        return model

        
