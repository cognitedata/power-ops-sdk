from __future__ import annotations

import ast
import json
import typing
from dataclasses import dataclass
from typing import ClassVar, Dict, Generator, List, Literal, Optional, Tuple

from cognite.client.data_classes import Asset, Label
from pydantic import BaseModel, ConfigDict, Field, model_validator, validator

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabels, RelationshipLabels
from cognite.powerops.bootstrap.data_classes.marked_configuration import PriceScenario, PriceScenarioID
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import (
    Configuration,
    RelativeTime,
    map_price_scenarios_by_name,
)
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection, write_mapping_to_sequence
from cognite.powerops.bootstrap.data_classes.shared import (
    AggregationMethod,
    Auction,
    RetrievalType,
    TimeSeriesMapping,
    TimeSeriesMappingEntry,
    Transformation,
    TransformationType,
)
from cognite.powerops.utils.cdf.resource_creation import simple_relationship

# TODO: Switch to class inheriting from str and Enum, or 3.11 strEnum?
Product = Literal["up", "down"]
Block = Literal["day", "night"]


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
            labels=[Label(AssetLabels.RKOM_BID_CONFIGURATION.value)],
        )

    def to_bootstrap_resources(self, price_scenarios_by_id, market_name) -> ResourceCollection:
        bootstrap_resources = ResourceCollection()
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
                label_external_id=RelationshipLabels.INCREMENTAL_MAPPING_SEQUENCE.value,
            )
            bootstrap_resources.add(relationship)

        return bootstrap_resources


@dataclass
class ReserveScenario:
    volume: int
    auction: Auction
    product: Product
    block: Block
    reserve_group: str
    mip_plant_time_series: List[
        Tuple[str, Optional[str]]
    ]  # (plant, mip_flag_time_series) for plants with generators that are in the reserve group
    obligation_external_id: Optional[str] = None

    def __post_init__(self):
        if len(self.mip_plant_time_series) == 0:
            raise ValueError("No `mip_plants` specified!")

    @property
    def obligation_transformations(self) -> List[Transformation]:
        # TODO: move some of this logic
        n_days = 5 if self.auction == "week" else 2
        night = self.block == "night"
        kwargs = generate_reserve_schedule(volume=self.volume, n_days=n_days, night=night)
        if self.obligation_external_id:
            return [Transformation(transformation=TransformationType.DYNAMIC_ADD_FROM_OFFSET, kwargs=kwargs)]
        else:
            return [Transformation(transformation=TransformationType.DYNAMIC_STATIC, kwargs=kwargs)]

    def mip_flag_transformations(self, mip_time_series: Optional[str]) -> List[Transformation]:
        if not mip_time_series:
            # Just run with_mip flag the entire period
            return [Transformation(transformation=TransformationType.STATIC, kwargs={"0": 1})]

        # Run with mip_flag during bid, in addition to what is already set
        n_days = 5 if self.auction == "week" else 2
        n_minutes = n_days * 24 * 60
        kwargs = {0: 1, n_minutes: 0}
        return [
            Transformation(transformation=TransformationType.DYNAMIC_ADD_FROM_OFFSET, kwargs=kwargs),
            Transformation(transformation=TransformationType.TO_BOOL),
        ]

    @property
    def obligation_object_type(self) -> str:
        return "reserve_group"

    @property
    def obligation_attribute_name(self) -> str:
        if self.product == "up":
            return "rr_up_obligation"
        elif self.product == "down":
            return "rr_down_obligation"

    def to_time_series_mapping(self) -> TimeSeriesMapping:
        obligation = TimeSeriesMappingEntry(
            object_type=self.obligation_object_type,
            object_name=self.reserve_group,
            attribute_name=self.obligation_attribute_name,
            time_series_external_id=self.obligation_external_id,
            transformations=self.obligation_transformations,
            retrieve=RetrievalType.RANGE if self.obligation_external_id else None,
        )

        mip_flags = [
            TimeSeriesMappingEntry(
                object_type="plant",
                object_name=plant_name,
                attribute_name="mip_flag",
                time_series_external_id=mip_time_series,
                transformations=self.mip_flag_transformations(mip_time_series),
                retrieve=RetrievalType.RANGE if mip_time_series else None,
                aggregation=AggregationMethod.max,  # TODO: or `AggregationMethod.first`?
            )
            for plant_name, mip_time_series in self.mip_plant_time_series
        ]

        return TimeSeriesMapping(rows=[obligation, *mip_flags])


# ! TODO: what about daylight saving ??


def generate_reserve_schedule(volume: int, n_days: int, night: bool = False) -> Dict[str, int]:
    """Creates the argument for the mapping transformation function for RKOM reserve obligation.
        Some examples:
            {"0": 10, "60": 0}
                --> Obligation of 10MW the first hour, then no obligation
            {"60": 10, "120": 0, "60": 10}
                --> No obligation the first hour (implicit), then 10 MW for one hour,
                    then no obligation for one hour, and finally 10 MW for the rest of the *period*

    Args:
        volume (int): volume in MW
        n_days (int): the number of days to generate the schedule for.
        night (bool, optional): True if the obligation is during the night. Defaults to False.

    Returns:
        Dict[str, int]: The "obligation schedule".
    """
    if volume == 0:
        return {"0": 0}

    res = {}
    minutes_five_hours = 5 * 60
    minutes_day = 24 * 60
    for day in range(n_days):
        midnight = day * minutes_day
        at_5AM = day * minutes_day + minutes_five_hours
        res[f"{midnight}"] = volume if night else 0
        res[f"{at_5AM}"] = 0 if night else volume
    if not night:
        # only needed if last value was != 0 (i.e. during day)
        res[f"{minutes_day * n_days}"] = 0
    return res
