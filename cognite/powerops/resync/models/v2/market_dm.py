from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field
from typing_extensions import TypeAlias

from cognite.powerops.client.data_classes import (
    BenchmarkBidApply,
    BenchmarkProcessApply,
    BidMatrixGeneratorApply,
    DayAheadBidApply,
    DayAheadProcessApply,
    NordPoolMarketApply,
    RKOMBidApply,
    RKOMBidCombinationApply,
    RKOMMarketApply,
    RKOMProcessApply,
)
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base import DataModel, PowerOpsGraphQLModel, T_Model

from .graphql_schemas import GRAPHQL_MODELS

ExternalID: TypeAlias = str


class BenchmarkMarketDataModel(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["benchmark"]
    benchmarking: list[BenchmarkProcessApply] = Field(default_factory=list)
    bids: dict[ExternalID, BenchmarkBidApply] = Field(default_factory=dict)

    @classmethod
    def from_cdf(
        cls: type[T_Model], client: PowerOpsClient, fetch_metadata: bool = True, fetch_content: bool = False
    ) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()


class DayAheadMarketDataModel(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["dayahead"]
    dayahead_processes: list[DayAheadProcessApply] = Field(default_factory=list)
    bids: dict[ExternalID, DayAheadBidApply] = Field(default_factory=dict)
    bid_matrix_generator: dict[ExternalID, BidMatrixGeneratorApply] = Field(default_factory=dict)
    nordpool_market: Optional[NordPoolMarketApply] = None

    @classmethod
    def from_cdf(
        cls: type[T_Model], client: PowerOpsClient, fetch_metadata: bool = True, fetch_content: bool = False
    ) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()


class RKOMMarketDataModel(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["rkom"]
    rkom_market: Optional[RKOMMarketApply] = None
    bids: dict[ExternalID, RKOMBidApply] = Field(default_factory=dict)
    rkom_bid_combinations: list[RKOMBidCombinationApply] = Field(default_factory=list)
    rkom_processes: list[RKOMProcessApply] = Field(default_factory=list)

    @classmethod
    def from_cdf(
        cls: type[T_Model], client: PowerOpsClient, fetch_metadata: bool = True, fetch_content: bool = False
    ) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()
