from __future__ import annotations

import ast
import json
import typing
from typing import ClassVar, Generator, List, Optional, Tuple

from cognite.client.data_classes import Asset, Label
from pydantic import BaseModel, ConfigDict, Field, model_validator, validator

from cognite.powerops._shared_data_classes import AssetLabels, RelationshipLabels
from cognite.powerops.bootstrap.data_classes.cdf_resource_collection import BootstrapResourceCollection
from cognite.powerops.bootstrap.data_classes.common import RelativeTime
from cognite.powerops.bootstrap.data_classes.marked_configuration import PriceScenario, PriceScenarioID
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import (
    Configuration,
    map_price_scenarios_by_name,
)
from cognite.powerops.bootstrap.data_classes.reserve_scenario import Block, Product, ReserveScenario
from cognite.powerops.bootstrap.data_classes.shared import Auction
from cognite.powerops.bootstrap.data_classes.time_series_mapping import write_mapping_to_sequence
from cognite.powerops.utils.cdf.resource_creation import simple_relationship


class RkomMarketConfig(BaseModel):
    external_id: str
    name: str
    timezone: str
    start_of_week: int
    parent_external_id: ClassVar[str] = "market_configurations"

    @property
    def metadata(self) -> dict:
        return {
            "timezone": self.timezone,
            "start_of_week": self.start_of_week,
        }

    @property
    def cdf_asset(self) -> Asset:
        return Asset(
            external_id=self.external_id,
            name=self.name,
            metadata=self.metadata,
            parent_external_id=self.parent_external_id,
            labels=["market"],
        )

    @staticmethod
    def default() -> "RkomMarketConfig":
        return RkomMarketConfig(
            external_id="market_configuration_statnett_rkom_weekly",
            name="RKOM weekly (Statnett)",
            timezone="Europe/Oslo",
            start_of_week=1,
        )


class RKOMBidCombinationConfig(Configuration):
    model_config = ConfigDict(populate_by_name=True)
    auction: Auction = Field(alias="bid_auction")
    name: str = Field("default", alias="bid_combination_name")
    rkom_bid_config_external_ids: List[str] = Field(alias="bid_rkom_bid_configs")
    parent_external_id: ClassVar[str] = "rkom_bid_combination_configurations"

    @validator("auction", pre=True)
    def to_enum(cls, value):
        return Auction[value] if isinstance(value, str) else value

    @validator("rkom_bid_config_external_ids", pre=True)
    def parse_string(cls, value):
        return [external_id for external_id in ast.literal_eval(value)] if isinstance(value, str) else value

    @property
    def cdf_asset(self) -> Asset:
        sequence_external_id = f"RKOM_bid_combination_configuration_{self.auction.value}_{self.name}"

        return Asset(
            name=sequence_external_id.replace("_", " "),
            description="Defining which RKOM bid methods should be combined (into the total bid form)",
            external_id=sequence_external_id,
            metadata={
                "bid:auction": self.auction.value,
                "bid:combination_name": self.name,
                "bid:rkom_bid_configs": json.dumps(self.rkom_bid_config_external_ids),
            },
            parent_external_id=self.parent_external_id,
        )


class RKOMBidProcessConfig(Configuration):
    watercourse: str = Field(alias="bid_watercourse")

    price_scenarios: List[PriceScenarioID] = Field(alias="bid_price_scenarios")
    reserve_scenarios: ReserveScenarios = Field(alias="bid_reserve_scenarios")

    shop_start: RelativeTime = Field(alias="shop_starttime")
    shop_end: RelativeTime = Field(alias="shop_endtime")

    timezone: str = "Europe/Oslo"
    method: str = "simple"

    minimum_price: int = 0  # TODO: need to specify currency
    price_premium: int = 0  # TODO: need to specify currency

    parent_external_id: typing.ClassVar[str] = "rkom_bid_process_configurations"
    mapping_type: ClassVar[str] = "rkom_incremental_mapping"

    @model_validator(mode="before")
    def create_reserve_scenarios(cls, value):
        if not isinstance(volumes := value.get("reserve_scenarios"), str):
            return value
        volumes = [int(volume.removesuffix("MW")) for volume in volumes[1:-1].split(",")]

        value["bid_reserve_scenarios"] = dict(
            volumes=volumes,
            auction=value["bid_auction"],
            product=value["bid_product"],
            block=value["bid_block"],
            reserve_group=value["labels"][0]["externalId"],
            mip_plant_time_series=[],
        )
        return value

    @validator("shop_start", "shop_end", pre=True)
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

    @validator("price_scenarios", pre=True)
    def literal_eval(cls, value):
        return [{"id": id_} for id_ in ast.literal_eval(value)] if isinstance(value, str) else value

    @property
    def sorted_volumes(self) -> List[int]:
        return sorted(self.reserve_scenarios.volumes)

    @property
    def name(self) -> str:
        return (
            f"{self.watercourse}_"
            f"{self.reserve_scenarios.auction.value}_"
            f"{self.reserve_scenarios.product}_"
            f"{self.reserve_scenarios.block}_"
            f"{len(self.price_scenarios)}-prices_"
            f"{self.sorted_volumes[1]}MW-{self.sorted_volumes[-1]}MW"
        )

    @property
    def external_id(self) -> str:
        return f"POWEROPS_{self.name}"

    @property
    def bid_date(self) -> RelativeTime:
        if self.reserve_scenarios.auction == "week":
            return RelativeTime(relative_time_string="monday")
        else:
            return RelativeTime(relative_time_string="saturday")

    @property
    def rkom_plants(self) -> List[str]:
        return [plant for plant, _ in self.reserve_scenarios.mip_plant_time_series]

    def to_metadata(self, rkom_price_scenarios_by_id: dict[str, PriceScenario], rkom_market_name: str) -> dict:
        price_scenarios = map_price_scenarios_by_name(
            self.price_scenarios, rkom_price_scenarios_by_id, rkom_market_name
        )

        return {
            "bid:watercourse": self.watercourse,
            "bid:auction": self.reserve_scenarios.auction.value,
            "bid:block": self.reserve_scenarios.block,
            "bid:product": self.reserve_scenarios.product,
            "bid:method": self.method,
            "bid:date": str(self.bid_date),
            "bid:price_scenarios": str(list(price_scenarios)),
            "bid:reserve_scenarios": str(self.reserve_scenarios),
            "bid:minimum_price": str(self.minimum_price),
            "bid:price_premium": str(self.price_premium),
            "rkom:plants": str(sorted(self.rkom_plants)),
            "shop:starttime": str(self.shop_start),
            "shop:endtime": str(self.shop_end),
            "timezone": self.timezone,
        }

    def to_cdf_asset(self, rkom_price_scenarios_by_id: dict[str, PriceScenario], rkom_market_name: str) -> Asset:
        return Asset(
            external_id=self.external_id,
            name=self.name,
            metadata=self.to_metadata(rkom_price_scenarios_by_id, rkom_market_name),
            description=f"RKOM bid generation config for {self.watercourse}",
            parent_external_id=self.parent_external_id,
            labels=[Label(AssetLabels.RKOM_BID_CONFIGURATION)],
        )

    def to_bootstrap_resources(self, price_scenarios_by_id, market_name) -> BootstrapResourceCollection:
        bootstrap_resources = BootstrapResourceCollection()
        asset = self.to_cdf_asset(price_scenarios_by_id, market_name)
        bootstrap_resources.add(asset)

        price_scenarios = map_price_scenarios_by_name(self.price_scenarios, price_scenarios_by_id, market_name)

        # Create incremental mapping for each combination of price scenario and reserve_scenario
        for price_scenario_name, price_scenario in price_scenarios.items():
            price_mapping = price_scenario.to_time_series_mapping()
            for reserve_scenario in self.reserve_scenarios:
                reserve_mapping = reserve_scenario.to_time_series_mapping()

                bootstrap_resources += write_mapping_to_sequence(
                    watercourse=self.watercourse,
                    mapping=price_mapping + reserve_mapping,
                    mapping_type=self.mapping_type,  # type: ignore
                    reserve_volume=reserve_scenario.volume,
                    price_scenario_name=price_scenario_name,
                    config_name=self.name,
                )

        for sequence in bootstrap_resources.sequences.values():
            relationship = simple_relationship(
                source=asset,
                target=sequence,
                label_external_id=RelationshipLabels.INCREMENTAL_MAPPING_SEQUENCE,
            )
            bootstrap_resources.add(relationship)

        return bootstrap_resources


class ReserveScenarios(BaseModel):
    volumes: list[int]
    auction: Auction
    product: Product
    block: Block
    reserve_group: str
    mip_plant_time_series: List[Tuple[str, Optional[str]]]
    obligation_external_id: Optional[str]

    @validator("auction", pre=True)
    def to_enum(cls, value):
        return Auction[value] if isinstance(value, str) else value

    @validator("volumes", pre=True)
    def valid_volumes(cls, volumes):
        if 0 not in volumes:
            raise ValueError("You probably want 0 MW as one of the volumes!")
        if any(volume < 0 for volume in volumes):
            raise ValueError(f"All volumes should be positive! Got {volumes}")
        return list(set(volumes))  # Do not want duplicate volumes

    def __str__(self) -> str:
        return f'[{"MW, ".join(str(volume) for volume in sorted(self.volumes))}MW]'  # E.g "[0MW, 10MW, 20MW]"

    def __len__(self) -> int:
        return len(self.list_scenarios())

    def __iter__(self) -> Generator[ReserveScenario, None, None]:  # type: ignore
        yield from self.list_scenarios()

    def list_scenarios(self) -> List[ReserveScenario]:
        return [
            ReserveScenario(
                volume=volume,
                auction=self.auction,
                product=self.product,
                block=self.block,
                reserve_group=self.reserve_group,
                mip_plant_time_series=self.mip_plant_time_series,
                obligation_external_id=self.obligation_external_id,
            )
            for volume in self.volumes
        ]
