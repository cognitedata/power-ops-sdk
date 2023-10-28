from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._bid_curves import BidCurvesApply
    from ._time_interval import TimeIntervalApply

__all__ = ["Periods", "PeriodsApply", "PeriodsList", "PeriodsApplyList", "PeriodsFields", "PeriodsTextFields"]


PeriodsTextFields = Literal["resolution"]
PeriodsFields = Literal["resolution"]

_PERIODS_PROPERTIES_BY_FIELD = {
    "resolution": "Resolution",
}


class Periods(DomainModel):
    space: str = "power-ops"
    resolution: Optional[str] = Field(None, alias="Resolution")
    time_interval: Optional[str] = Field(None, alias="TimeInterval")
    bid_curves: Optional[list[str]] = Field(None, alias="BidCurves")

    def as_apply(self) -> PeriodsApply:
        return PeriodsApply(
            external_id=self.external_id,
            resolution=self.resolution,
            time_interval=self.time_interval,
            bid_curves=self.bid_curves,
        )


class PeriodsApply(DomainModelApply):
    space: str = "power-ops"
    resolution: Optional[str] = Field(None, alias="Resolution")
    time_interval: Union[TimeIntervalApply, str, None] = Field(None, repr=False, alias="TimeInterval")
    bid_curves: Union[list[BidCurvesApply], list[str], None] = Field(default=None, repr=False, alias="BidCurves")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.resolution is not None:
            properties["Resolution"] = self.resolution
        if self.time_interval is not None:
            properties["TimeInterval"] = {
                "space": "power-ops",
                "externalId": self.time_interval
                if isinstance(self.time_interval, str)
                else self.time_interval.external_id,
            }
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

        for bid_curve in self.bid_curves or []:
            edge = self._create_bid_curve_edge(bid_curve)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(bid_curve, DomainModelApply):
                instances = bid_curve._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.time_interval, DomainModelApply):
            instances = self.time_interval._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_bid_curve_edge(self, bid_curve: Union[str, BidCurvesApply]) -> dm.EdgeApply:
        if isinstance(bid_curve, str):
            end_node_ext_id = bid_curve
        elif isinstance(bid_curve, DomainModelApply):
            end_node_ext_id = bid_curve.external_id
        else:
            raise TypeError(f"Expected str or BidCurvesApply, got {type(bid_curve)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ReserveBidTimeSeries.BidCurves"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PeriodsList(TypeList[Periods]):
    _NODE = Periods

    def as_apply(self) -> PeriodsApplyList:
        return PeriodsApplyList([node.as_apply() for node in self.data])


class PeriodsApplyList(TypeApplyList[PeriodsApply]):
    _NODE = PeriodsApply
