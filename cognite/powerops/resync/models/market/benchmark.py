from __future__ import annotations


from typing import ClassVar, Union

from cognite.client.data_classes import TimeSeries
from pydantic import Field, field_validator, model_validator, model_serializer

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import NonAssetType

from .base import Bid, Process, ShopTransformation
from cognite.powerops.resync.utils.serializer import try_load_dict, try_load_list


class BenchmarkBid(Bid):
    market_config_external_id: str


class ProductionPlanTimeSeries(NonAssetType):
    name: str
    series: list[TimeSeries]

    @model_validator(mode="before")
    def parse_dict(cls, value) -> dict:
        if isinstance(value, dict):
            name, series = next(iter(value.items()))
            return {"name": name, "series": [TimeSeries(external_id=s) for s in series]}
        return value

    @model_serializer()
    def ser_dict(self) -> dict:
        return {self.name: [s.external_id for s in self.series]}


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

    @field_validator("benchmarking_metrics", mode="before")
    def parse_str(cls, value) -> dict:
        return try_load_dict(value)

    @field_validator("production_plan_time_series", mode="before")
    def parse_str_to_list(cls, value) -> list:
        if isinstance(loaded := try_load_list(value), dict):
            return [loaded]
        return loaded
