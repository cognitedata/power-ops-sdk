from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from cognite.powerops.bootstrap.config_classes.cdf_labels import AssetLabel
from cognite.powerops.bootstrap.models.base import CDFSequence, NonAssetType, Type
from cognite.powerops.bootstrap.models.market.base import Bid, Market, Process


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
    bid: RKOMBid
    process_events: list[str] = Field(default_factory=list)
    timezone: str
    rkom: RKOMPlants
    incremental_mapping: list[CDFSequence] = Field(default_factory=list)


class RKOMCombinationBid(NonAssetType):
    auction: str
    combination_name: str
    rkom_bid_configs: list[str]


class RKOMBidCombination(Type):
    type_: ClassVar[str] = "rkom_bid_combination_configuration"
    label: ClassVar[str] = AssetLabel.RKOM_BID_CONFIGURATION
    bid: RKOMCombinationBid


class RKOMMarket(Market):
    start_of_week: int
