from __future__ import annotations

from typing import ClassVar, Union

from cognite.client.data_classes import TimeSeries
from pydantic import Field, field_validator, model_serializer, model_validator

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import NonAssetType
from cognite.powerops.utils.serialization import try_load_dict, try_load_list

from .base import Bid, Process, ShopTransformation


class BenchmarkBid(Bid):
    market_config_external_id: str


class ProductionPlanTimeSeries(NonAssetType):
    name: str
    series: list[TimeSeries]

    @field_validator("series", mode="after")
    def ordering(cls, value: list[TimeSeries]) -> list[TimeSeries]:
        return sorted(value, key=lambda x: x.external_id)

    @model_validator(mode="before")
    def parse_dict(cls, value) -> dict:
        if isinstance(value, dict):
            name, series = next(iter(value.items()))
            return {"name": name, "series": [TimeSeries(external_id=s) for s in series]}
        return value

    @model_serializer()
    def ser_dict(self) -> dict:
        return {self.name: [s.external_id for s in self.series]}

    def standardize(self) -> None:
        self.series = self.ordering(self.series)


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
    bid_process_configuration_assets: list[Process] = Field(default_factory=list)

    @field_validator("production_plan_time_series", mode="after")
    def ordering(cls, value: list[ProductionPlanTimeSeries]) -> list[ProductionPlanTimeSeries]:
        return sorted(value, key=lambda x: x.name)

    @field_validator("run_events", mode="after")
    def ordering_events(cls, value: list[str]) -> list[str]:
        return sorted(value)

    @field_validator("bid_process_configuration_assets", mode="after")
    def ordering_processes(cls, value: list[Process]) -> list[Process]:
        return sorted(value, key=lambda x: x.name)

    @field_validator("benchmarking_metrics", mode="before")
    def parse_str(cls, value) -> dict:
        return try_load_dict(value)

    @field_validator("production_plan_time_series", mode="before")
    def parse_str_to_list(cls, value) -> list:
        if isinstance(loaded := try_load_list(value), dict):
            return [loaded]
        return loaded

    def standardize(self) -> None:
        self.production_plan_time_series = self.ordering(self.production_plan_time_series)
        self.run_events = self.ordering_events(self.run_events)
        self.bid_process_configuration_assets = self.ordering_processes(self.bid_process_configuration_assets)
