from __future__ import annotations

from typing import ClassVar

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel
from cognite.powerops.bootstrap.models.base import CDFSequence, NonAssetType, Type
from cognite.powerops.bootstrap.models.market.base import Bid, Market, Process


class RKOMBid(Bid):
    auction: str
    block: str
    method: str
    minimum_price: float
    price_premium: float
    price_scenarios: list[str]
    product: str
    reserve_scenarios: list[str]
    watercourse: str


class RKOMProcess(Process):
    type_ = "POWEROPS"
    bid: RKOMBid
    process_events: list[str]
    timezone: str
    incremental_mapping: list[CDFSequence]


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
