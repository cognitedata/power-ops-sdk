from __future__ import annotations

from typing import ClassVar, Union

from pydantic import Field, field_validator

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence

from .base import Bid, Market, Process, ShopTransformation
from ...utils.serializer import try_load_list


class RKOMBid(Bid):
    auction: str
    block: str
    method: str
    minimum_price: str
    price_premium: str
    product: str
    watercourse: str
    price_scenarios: str
    reserve_scenarios: str


class RKOMPlants(NonAssetType):
    plants: str


class RKOMProcess(Process):
    parent_external_id: ClassVar[str] = "rkom_bid_process_configurations"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations used in RKOM bid generation processes"
    shop: ShopTransformation
    bid: RKOMBid
    process_events: list[str] = Field(default_factory=list)
    timezone: str
    rkom: RKOMPlants
    incremental_mapping: list[CDFSequence] = Field(default_factory=list)

    @field_validator("process_events", mode="before")
    def parse_str(cls, value) -> list:
        return try_load_list(value)


class RKOMCombinationBid(NonAssetType):
    auction: str
    combination_name: str
    rkom_bid_configs: list[str]

    @field_validator("rkom_bid_configs", mode="before")
    def parse_str(cls, value) -> list:
        return try_load_list(value)


class RKOMBidCombination(AssetType):
    parent_external_id: ClassVar[str] = "rkom_bid_combination_configurations"
    label: ClassVar[str] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations for which bids should be combined into a total RKOM bid form"
    bid: RKOMCombinationBid


class RKOMMarket(Market):
    start_of_week: int
