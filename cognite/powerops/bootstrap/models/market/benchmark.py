from dataclasses import dataclass

from cognite.client.data_classes import TimeSeries

from cognite.powerops.bootstrap.models.base import Type
from cognite.powerops.bootstrap.models.market.base import Bid, Process


@dataclass
class BenchmarkBid(Bid):
    ...


@dataclass
class ProductionPlanTimeSeries(Type):
    series: list[TimeSeries]


@dataclass
class BenchmarkProcess(Process):
    type_ = "benchmarking_configurations"
    bid: BenchmarkBid
    production_plan_time_series: list[ProductionPlanTimeSeries]
    metrics: list[str]
    run_events: list[str]
