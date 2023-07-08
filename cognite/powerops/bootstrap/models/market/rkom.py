from __future__ import annotations

from cognite.powerops.bootstrap.models.base import CDFSequence, Type
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


class RKOMBidCombination(Type):
    type_ = "RKOM_bid_combination_configuration"
    auction: str
    bid_configurations: list[RKOMProcess]


class RKOMMarket(Market):
    start_of_week: int
