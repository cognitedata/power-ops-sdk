from __future__ import annotations

from pydantic import Field

from cognite.powerops.clients.market.data_classes import (
    BenchmarkingApply,
    DayAheadProcesApply,
    NordPoolMarketApply,
    RKOMBidCombinationApply,
    RKOMMarketApply,
    RKOMProces,
)
from cognite.powerops.resync.models._base import DataModel


class MarketDM(DataModel):
    rkom_market: list[RKOMMarketApply] = Field(default_factory=list)
    nordpool_market: list[NordPoolMarketApply] = Field(default_factory=list)
    rkom_proces: list[RKOMProces] = Field(default_factory=list)
    rkom_bid_combination: list[RKOMBidCombinationApply] = Field(default_factory=list)
    day_ahead_proces: list[DayAheadProcesApply] = Field(default_factory=list)
    benchmarking: list[BenchmarkingApply] = Field(default_factory=list)
