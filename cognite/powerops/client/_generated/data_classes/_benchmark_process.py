from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._benchmark_bid import BenchmarkBidApply
    from ._production_plan_time_series import ProductionPlanTimeSeriesApply
    from ._shop_transformation import ShopTransformationApply

__all__ = [
    "BenchmarkProcess",
    "BenchmarkProcessApply",
    "BenchmarkProcessList",
    "BenchmarkProcessApplyList",
    "BenchmarkProcessFields",
    "BenchmarkProcessTextFields",
]


BenchmarkProcessTextFields = Literal["name", "run_events"]
BenchmarkProcessFields = Literal["name", "metrics", "run_events"]

_BENCHMARKPROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
    "metrics": "metrics",
    "run_events": "runEvents",
}


class BenchmarkProcess(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    metrics: Optional[dict] = None
    bid: Optional[str] = None
    shop: Optional[str] = None
    run_events: Optional[list[str]] = Field(None, alias="runEvents")
    production_plan_time_series: Optional[list[str]] = Field(None, alias="productionPlanTimeSeries")

    def as_apply(self) -> BenchmarkProcessApply:
        return BenchmarkProcessApply(
            external_id=self.external_id,
            name=self.name,
            metrics=self.metrics,
            bid=self.bid,
            shop=self.shop,
            run_events=self.run_events,
            production_plan_time_series=self.production_plan_time_series,
        )


class BenchmarkProcessApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    metrics: Optional[dict] = None
    bid: Union[BenchmarkBidApply, str, None] = Field(None, repr=False)
    shop: Union[ShopTransformationApply, str, None] = Field(None, repr=False)
    run_events: Optional[list[str]] = Field(None, alias="runEvents")
    production_plan_time_series: Union[list[ProductionPlanTimeSeriesApply], list[str], None] = Field(
        default=None, repr=False, alias="productionPlanTimeSeries"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.metrics is not None:
            properties["metrics"] = self.metrics
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.shop is not None:
            properties["shop"] = {
                "space": "power-ops",
                "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
            }
        if self.run_events is not None:
            properties["runEvents"] = self.run_events
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BenchmarkProcess"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for production_plan_time_series in self.production_plan_time_series or []:
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_production_plan_time_series_edge(
        self, production_plan_time_series: Union[str, ProductionPlanTimeSeriesApply]
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
            type=dm.DirectRelationReference("power-ops", "BenchmarkProcess.productionPlanTimeSeries"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class BenchmarkProcessList(TypeList[BenchmarkProcess]):
    _NODE = BenchmarkProcess

    def as_apply(self) -> BenchmarkProcessApplyList:
        return BenchmarkProcessApplyList([node.as_apply() for node in self.data])


class BenchmarkProcessApplyList(TypeApplyList[BenchmarkProcessApply]):
    _NODE = BenchmarkProcessApply
