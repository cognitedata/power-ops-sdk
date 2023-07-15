from __future__ import annotations

from pydantic import Field

from cognite.powerops.clients.data_classes import (
    BenchmarkBidApply,
    BenchmarkProcesApply,
    DayAheadProcesApply,
    NordPoolMarketApply,
    RKOMBidCombinationApply,
    RKOMMarketApply,
    RKOMProces,
)
from cognite.powerops.resync.models.base import DataModel

ExternalID = str


class BenchmarkMarketDataModel(DataModel):
    benchmarking: list[BenchmarkProcesApply] = Field(default_factory=list)
    bids: dict[ExternalID, BenchmarkBidApply] = Field(default_factory=dict)


class MarketDM(DataModel):
    rkom_market: list[RKOMMarketApply] = Field(default_factory=list)
    nordpool_market: list[NordPoolMarketApply] = Field(default_factory=list)
    rkom_proces: list[RKOMProces] = Field(default_factory=list)
    rkom_bid_combination: list[RKOMBidCombinationApply] = Field(default_factory=list)
    dayahead_process: list[DayAheadProcesApply] = Field(default_factory=list)
    benchmarking: list[BenchmarkProcesApply] = Field(default_factory=list)
