from typing import ClassVar, Optional

from cognite.client.data_classes import Asset
from pydantic.dataclasses import Field

from cognite.powerops.resync.models.base import Model

from .base import Bid, DateTransformation, Market, Process, ShopTransformation
from .benchmark import BenchmarkBid, BenchmarkProcess, ProductionPlanTimeSeries
from .dayahead import DayAheadBid, DayAheadProcess, NordPoolMarket
from .rkom import RKOMBid, RKOMBidCombination, RKOMCombinationBid, RKOMMarket, RKOMPlants, RKOMProcess


class MarketModel(Model):
    root_asset: ClassVar[Asset] = None
    markets: list[Market] = Field(default_factory=list)
    processes: list[Process] = Field(default_factory=list)
    combinations: list[RKOMBidCombination] = Field(default_factory=list)

    @classmethod
    def set_root_asset(
        cls,
    ):
        raise NotImplementedError()


__all__ = [
    "RKOMBid",
    "RKOMBidCombination",
    "RKOMMarket",
    "RKOMProcess",
    "RKOMCombinationBid",
    "RKOMPlants",
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
