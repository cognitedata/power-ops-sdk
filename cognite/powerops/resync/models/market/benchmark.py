from __future__ import annotations

from typing import ClassVar

from cognite.client.data_classes import TimeSeries
from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models._base import NonAssetType
from cognite.powerops.resync.models.market.base import Bid, Process


class BenchmarkBid(Bid):
    market_config_external_id: str


class ProductionPlanTimeSeries(NonAssetType):
    name: str
    series: list[TimeSeries]


class BenchmarkProcess(Process):
    type_: ClassVar[str] = "benchmarking_configuration"
    parent_description: ClassVar[str] = "Configurations used in benchmarking processes"
    description: str = "Configuration for benchmarking of day-ahead bidding"
    label: ClassVar[AssetLabel] = AssetLabel.DAYAHEAD_BIDDING_BENCHMARKING_CONFIG
    bid: BenchmarkBid
    production_plan_time_series: list[ProductionPlanTimeSeries] = Field(default_factory=list)
    benchmarking_metrics: dict[str, str] = Field(default_factory=dict)
    run_events: list[str] = Field(default_factory=list)
