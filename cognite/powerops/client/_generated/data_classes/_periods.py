from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._reserve_bid_time_series import ReserveBidTimeSeriesApply

__all__ = ["Periods", "PeriodsApply", "PeriodsList", "PeriodsApplyList", "PeriodsFields", "PeriodsTextFields"]


PeriodsTextFields = Literal["resolution"]
PeriodsFields = Literal["resolution"]

_PERIODS_PROPERTIES_BY_FIELD = {
    "resolution": "Resolution",
}


class Periods(DomainModel):
    space: str = "power-ops"
    resolution: Optional[list[str]] = Field(None, alias="Resolution")
    parent: Optional[list[str]] = None

    def as_apply(self) -> PeriodsApply:
        return PeriodsApply(
            external_id=self.external_id,
            resolution=self.resolution,
            parent=self.parent,
        )


class PeriodsApply(DomainModelApply):
    space: str = "power-ops"
    resolution: Optional[list[str]] = Field(None, alias="Resolution")
    parent: Union[list[ReserveBidTimeSeriesApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.resolution is not None:
            properties["Resolution"] = self.resolution
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Periods"),
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

        for parent in self.parent or []:
            edge = self._create_parent_edge(parent)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(parent, DomainModelApply):
                instances = parent._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_parent_edge(self, parent: Union[str, ReserveBidTimeSeriesApply]) -> dm.EdgeApply:
        if isinstance(parent, str):
            end_node_ext_id = parent
        elif isinstance(parent, DomainModelApply):
            end_node_ext_id = parent.external_id
        else:
            raise TypeError(f"Expected str or ReserveBidTimeSeriesApply, got {type(parent)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Periods.parent"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PeriodsList(TypeList[Periods]):
    _NODE = Periods

    def as_apply(self) -> PeriodsApplyList:
        return PeriodsApplyList([node.as_apply() for node in self.data])


class PeriodsApplyList(TypeApplyList[PeriodsApply]):
    _NODE = PeriodsApply
