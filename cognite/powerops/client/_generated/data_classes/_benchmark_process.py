from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._benchmark_bids import BenchmarkBidApply
    from cognite.powerops.client._generated.data_classes._production_plan_time_series import (
        ProductionPlanTimeSeriesApply,
    )
    from cognite.powerops.client._generated.data_classes._shop_transformations import ShopTransformationApply

__all__ = ["BenchmarkProces", "BenchmarkProcesApply", "BenchmarkProcesList"]


class BenchmarkProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    metrics: Optional[dict] = None
    name: Optional[str] = None
    production_plan_time_series: list[str] = Field([], alias="productionPlanTimeSeries")
    run_events: list[str] = Field([], alias="runEvents")
    shop: Optional[str] = None


class BenchmarkProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[BenchmarkBidApply, str]] = Field(None, repr=False)
    metrics: Optional[dict] = None
    name: Optional[str] = None
    production_plan_time_series: list[Union[ProductionPlanTimeSeriesApply, str]] = Field(
        default_factory=list, repr=False
    )
    run_events: list[str] = []
    shop: Optional[Union[ShopTransformationApply, str]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.metrics is not None:
            properties["metrics"] = self.metrics
        if self.name is not None:
            properties["name"] = self.name
        if self.run_events is not None:
            properties["runEvents"] = self.run_events
        if self.shop is not None:
            properties["shop"] = {
                "space": "power-ops",
                "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
            }
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


class BenchmarkProcesList(TypeList[BenchmarkProces]):
    _NODE = BenchmarkProces
