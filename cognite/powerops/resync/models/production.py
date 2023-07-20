from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.utils.cdf.calls import retrieve_relationships_from_source_ext_id


class Generator(AssetType):
    type_: ClassVar[str] = "generator"
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
                external_id=asset.external_id,
                name=asset.name,
                description=asset.description,
                labels=asset.labels,
                p_min=float(asset.metadata.get("p_min", 0.0)),
                penstock=asset.metadata.get("penstock", ""),
                startcost=float(asset.metadata.get("startcost", 0.0)),
                start_stop_cost_time_series=None,
                generator_efficiency_curve=None,
                turbine_efficiency_curve=None
            )

    # @classmethod
    # def from_cdf(cls, client: CogniteClient, root_generator_asset: Asset, ) -> list[Generator]:
    #     asset_subtree = client.assets.retrieve_subtree(root_generator_asset.id)
    #     return [ Generator.from_asset(client, asset) for asset in asset_subtree if asset.id != root_generator_asset.id ]

    


class Reservoir(AssetType):
    type_: ClassVar[str] = "reservoir"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str
    ordering: str

    @classmethod
    def from_asset(cls, asset: Asset) -> Reservoir:
        return cls(
                external_id=asset.external_id,
                name=asset.name,
                description=asset.description,
                labels=asset.labels,
                display_name=asset.metadata.get("display_name", ""),
                ordering=asset.metadata.get("ordering", ""),
            )
    

class Plant(AssetType):
    type_: ClassVar[str] = "plant"
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
    def from_asset(cls, asset: Asset) -> PriceArea:
        return cls(
                external_id=asset.external_id,
                name=asset.name,
                description=asset.description,
                labels=asset.labels,
                display_name=asset.metadata.get("display_name", ""),
                ordering=asset.metadata.get("ordering", ""),
                head_loss_factor=float(asset.metadata.get("head_loss_factor", 0.0)),
                outlet_level=float(asset.metadata.get("outlet_level", 0.0)),
                p_min=float(asset.metadata.get("p_min", 0.0)),
                p_max=float(asset.metadata.get("p_max", 0.0)),
                penstock_head_loss_factors=asset.metadata.get("penstock_head_loss_factors", {}),
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
    type_: ClassVar[str] = "watercourse"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.WATERCOURSE
    shop: WaterCourseShop
    config_version: str = Field(exclude=True)
    model_file: Path = Field(exclude=True)
    processed_model_file: Path = Field(exclude=True)
    plants: list[Plant]
    production_obligation_time_series: list[TimeSeries] = Field(default_factory=list)

    # @classmethod
    # def from_cdf(cls: Watercourse, client:CogniteClient, root_watercourse_asset: Asset,) -> list[Watercourse]:
    #     watercourses = []
    #     asset_subtree = client.assets.retrieve_subtree(root_watercourse_asset.id)
    #     print(asset_subtree)
    #     watercourses.extend(
    #         cls(
    #             external_id=watercourse_asset.external_id,
    #             name=watercourse_asset.name,
    #             description=watercourse_asset.description,
    #             shop=WaterCourseShop(
    #                 penalty_limit=watercourse_asset.metadata.get(
    #                     "penalty_limit", ""
    #                 )
    #             ),
    #             # Are these storied in cdf at all?
    #             config_version=watercourse_asset.metadata.get(
    #                 "config_version", ""
    #             ),
    #             model_file=watercourse_asset.metadata.get("model_file", ""),
    #             processed_model_file=watercourse_asset.metadata.get(
    #                 "processed_model_file", ""
    #             ),
    #             plants=[],
    #             production_obligation_time_series=[],
    #         )
    #         for watercourse_asset in asset_subtree
    #         if watercourse_asset.external_id != root_watercourse_asset.external_id
    #     )
    #     return watercourses

        


class PriceArea(AssetType):
    type_: ClassVar[str] = "price_area"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    @classmethod
    def from_asset(cls, asset: Asset) -> PriceArea:
        return cls(
                # external_id=asset.external_id,
                # name=asset.name,
                # description=asset.description,
                # labels=asset.labels,
            )
    @classmethod
    def from_asset(cls, asset: Asset) -> PriceArea:
        return cls(
            external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            labels=asset.labels,
            dayahead_price_time_series=None,
            plants=[],
            watercourses=[],
        )
        


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    price_areas: list[PriceArea] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    plants: list[Plant] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    reservoirs: list[Reservoir] = Field(default_factory=list)

    
    # @classmethod
    # def from_cdf(cls: ProductionModel, client: CogniteClient) -> ProductionModel:
    #     model = cls()
    #     asset_subtree = client.assets.retrieve_subtree(external_id=cls.root_asset.external_id, depth=1)
    #     for asset in asset_subtree:
    #         asset_ext_id = asset.external_id
    #         print("asset_ext_id", asset_ext_id)
    #         if asset.external_id == "power_ops":
    #             continue
    #         if asset_ext_id == "generators":
    #             model.generators = Generator.from_cdf(client, asset)
    #         elif asset_ext_id == "reservoirs":
    #             model.reservoirs = Reservoir.from_cdf(client, asset)
    #         # elif asset.external_id == "watercourses":
    #         #     model.watercourses = [Watercourse.from_cdf(client, asset)]
    #         print("----------")

    #     return model
    


        
