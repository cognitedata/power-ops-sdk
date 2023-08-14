from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["ProductionPlanTimeSeries", "ProductionPlanTimeSeriesApply", "ProductionPlanTimeSeriesList"]


class ProductionPlanTimeSeries(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    series: list[str] = []


class ProductionPlanTimeSeriesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    series: list[str] = []

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ProductionPlanTimeSeries"),
            properties={
                "name": self.name,
                "series": self.series,
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
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProductionPlanTimeSeriesList(TypeList[ProductionPlanTimeSeries]):
    _NODE = ProductionPlanTimeSeries
