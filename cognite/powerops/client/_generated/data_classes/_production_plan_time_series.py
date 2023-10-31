from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "ProductionPlanTimeSeries",
    "ProductionPlanTimeSeriesApply",
    "ProductionPlanTimeSeriesList",
    "ProductionPlanTimeSeriesApplyList",
    "ProductionPlanTimeSeriesFields",
    "ProductionPlanTimeSeriesTextFields",
]


ProductionPlanTimeSeriesTextFields = Literal["name", "series"]
ProductionPlanTimeSeriesFields = Literal["name", "series"]

_PRODUCTIONPLANTIMESERIES_PROPERTIES_BY_FIELD = {
    "name": "name",
    "series": "series",
}


class ProductionPlanTimeSeries(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    series: Optional[list[str]] = None

    def as_apply(self) -> ProductionPlanTimeSeriesApply:
        return ProductionPlanTimeSeriesApply(
            external_id=self.external_id,
            name=self.name,
            series=self.series,
        )


class ProductionPlanTimeSeriesApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    series: Optional[list[str]] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.series is not None:
            properties["series"] = self.series
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ProductionPlanTimeSeries"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProductionPlanTimeSeriesList(TypeList[ProductionPlanTimeSeries]):
    _NODE = ProductionPlanTimeSeries

    def as_apply(self) -> ProductionPlanTimeSeriesApplyList:
        return ProductionPlanTimeSeriesApplyList([node.as_apply() for node in self.data])


class ProductionPlanTimeSeriesApplyList(TypeApplyList[ProductionPlanTimeSeriesApply]):
    _NODE = ProductionPlanTimeSeriesApply
