from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models._base import AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.market.base import Bid, Market, Process


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
    type_: ClassVar[str] = "POWEROPS"
    label: ClassVar[AssetLabel] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations used in RKOM bid generation processes"
    bid: RKOMBid
    process_events: list[str] = Field(default_factory=list)
    timezone: str
    rkom: RKOMPlants
    incremental_mapping: list[CDFSequence] = Field(default_factory=list)


class RKOMCombinationBid(NonAssetType):
    auction: str
    combination_name: str
    rkom_bid_configs: list[str]


class RKOMBidCombination(AssetType):
    type_: ClassVar[str] = "rkom_bid_combination_configuration"
    label: ClassVar[str] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations for which bids should be combined into a total RKOM bid form"
    bid: RKOMCombinationBid


class RKOMMarket(Market):
    start_of_week: int
