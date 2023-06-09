from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._benchmark_bids import BenchmarkBidApply
    from ._production_plan_time_series import ProductionPlanTimeSeriesApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["Benchmarking", "BenchmarkingApply", "BenchmarkingList"]


class Benchmarking(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    metrics: list[str] = []
    name: Optional[str] = None
    production_plan_time_series: list[str] = Field([], alias="productionPlanTimeSeries")
    run_events: list[str] = Field([], alias="runEvents")
    shop: Optional[str] = None


class BenchmarkingApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[str, "BenchmarkBidApply"]] = Field(None, repr=False)
    metrics: list[str] = []
    name: Optional[str] = None
    production_plan_time_series: list[Union[str, "ProductionPlanTimeSeriesApply"]] = Field(
        default_factory=lambda: [], repr=False
    )
    run_events: list[str] = []
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Benchmarking"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "metrics": self.metrics,
                "name": self.name,
                "runEvents": self.run_events,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        for production_plan_time_series in self.production_plan_time_series:
            edge = self._create_production_plan_time_series_edge(production_plan_time_series)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(production_plan_time_series, DomainModelApply):
                instances = production_plan_time_series._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_production_plan_time_series_edge(
        self, production_plan_time_series: Union[str, "ProductionPlanTimeSeriesApply"]
    ) -> dm.EdgeApply:
        if isinstance(production_plan_time_series, str):
            end_node_ext_id = production_plan_time_series
        elif isinstance(production_plan_time_series, DomainModelApply):
            end_node_ext_id = production_plan_time_series.external_id
        else:
            raise TypeError(f"Expected str or ProductionPlanTimeSeriesApply, got {type(production_plan_time_series)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Benchmarking.productionPlanTimeSeries"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class BenchmarkingList(TypeList[Benchmarking]):
    _NODE = Benchmarking
