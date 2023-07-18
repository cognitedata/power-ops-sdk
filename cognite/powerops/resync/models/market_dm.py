from __future__ import annotations

from typing import Optional

from pydantic import Field

from cognite.powerops.clients.data_classes import (
    BenchmarkBidApply,
    BenchmarkProcesApply,
    BidMatrixGeneratorApply,
    DayAheadBidApply,
    DayAheadProcesApply,
    NordPoolMarketApply,
    RKOMBidApply,
    RKOMBidCombinationApply,
    RKOMMarketApply,
    RKOMProcesApply,
)
from cognite.powerops.resync.models.base import DataModel

ExternalID = str


class BenchmarkMarketDataModel(DataModel):
    benchmarking: list[BenchmarkProcesApply] = Field(default_factory=list)
    bids: dict[ExternalID, BenchmarkBidApply] = Field(default_factory=dict)


class DayAheadMarketDataModel(DataModel):
    dayahead_processes: list[DayAheadProcesApply] = Field(default_factory=list)
    bids: dict[ExternalID, DayAheadBidApply] = Field(default_factory=dict)
    bid_matrix_generator: dict[ExternalID, BidMatrixGeneratorApply] = Field(default_factory=dict)
    nordpool_market: Optional[NordPoolMarketApply] = None


class RKOMMarketDataModel(DataModel):
    rkom_market: Optional[RKOMMarketApply] = None
    bids: dict[ExternalID, RKOMBidApply] = Field(default_factory=dict)
    rkom_bid_combinations: list[RKOMBidCombinationApply] = Field(default_factory=list)
    rkom_processes: list[RKOMProcesApply] = Field(default_factory=list)
