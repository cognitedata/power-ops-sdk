from __future__ import annotations

from typing import ClassVar, Union

from cognite.client.data_classes import TimeSeries
from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import NonAssetType

from .base import Bid, Process, ShopTransformation


class BenchmarkBid(Bid):
    market_config_external_id: str


class ProductionPlanTimeSeries(NonAssetType):
    name: str
    series: list[TimeSeries]


class BenchmarkProcess(Process):
    parent_external_id: ClassVar[str] = "benchmarking_configurations"
    parent_description: ClassVar[str] = "Configurations used in benchmarking processes"
    description: str = "Configuration for benchmarking of day-ahead bidding"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.DAYAHEAD_BIDDING_BENCHMARKING_CONFIG
    shop: ShopTransformation
    bid: BenchmarkBid
    production_plan_time_series: list[ProductionPlanTimeSeries] = Field(default_factory=list)
    benchmarking_metrics: dict[str, str] = Field(default_factory=dict)
    run_events: list[str] = Field(default_factory=list)
