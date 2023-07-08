from __future__ import annotations

from cognite.powerops.bootstrap.models.base import CDFSequence
from cognite.powerops.bootstrap.models.market.base import Bid, Market, Process, ShopTransformation


class DayAheadBid(Bid):
    is_default_config_for_price_area: bool
    main_scenario: str
    price_area: str
    price_scenarios: dict[str, str]
    shop: ShopTransformation


class DayAheadProcess(Process):
    type_ = "POWEROPS_bid_process_configuration"
    bid: DayAheadBid
    incremental_mapping: list[CDFSequence]


class NordPoolMarket(Market):
    max_price: float
    min_price: float
    price_steps: int
    price_unit: str
    tick_size: float
    time_unit: str
    trade_lot: float
