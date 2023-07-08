from dataclasses import dataclass, field

from cognite.powerops.bootstrap.models.base import Model

from .base import Bid, DateTransformation, Market, Process, ShopTransformation
from .benchmark import BenchmarkBid, BenchmarkProcess, ProductionPlanTimeSeries
from .dayahead import DayAheadBid, DayAheadProcess, NordPoolMarket
from .rkom import RKOMBid, RKOMBidCombination, RKOMMarket, RKOMProcess


@dataclass
class MarketConfig(Model):
    markets: list[Market] = field(default_factory=list)
    bids: list[Bid] = field(default_factory=list)
    processes: list[Process] = field(default_factory=list)
    combinations: list[RKOMBidCombination] = field(default_factory=list)


__all__ = [
    "RKOMBid",
    "RKOMBidCombination",
    "RKOMMarket",
    "RKOMProcess",
    "DayAheadBid",
    "NordPoolMarket",
    "DayAheadProcess",
    "BenchmarkBid",
    "BenchmarkProcess",
    "ProductionPlanTimeSeries",
    "Market",
    "DateTransformation",
    "ShopTransformation",
    "Bid",
    "Process",
    "MarketConfig",
]
