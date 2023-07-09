from pydantic.dataclasses import Field

from cognite.powerops.bootstrap.models.base import Model

from .base import Bid, DateTransformation, Market, Process, ShopTransformation
from .benchmark import BenchmarkBid, BenchmarkProcess, ProductionPlanTimeSeries
from .dayahead import DayAheadBid, DayAheadProcess, NordPoolMarket
from .rkom import RKOMBid, RKOMBidCombination, RKOMCombinationBid, RKOMMarket, RKOMProcess


class MarketModel(Model):
    markets: list[Market] = Field(default_factory=list)
    processes: list[Process] = Field(default_factory=list)
    combinations: list[RKOMBidCombination] = Field(default_factory=list)


__all__ = [
    "RKOMBid",
    "RKOMBidCombination",
    "RKOMMarket",
    "RKOMProcess",
    "RKOMCombinationBid",
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
    "MarketModel",
]
