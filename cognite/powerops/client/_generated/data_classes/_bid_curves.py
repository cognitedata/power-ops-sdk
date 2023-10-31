from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "BidCurves",
    "BidCurvesApply",
    "BidCurvesList",
    "BidCurvesApplyList",
    "BidCurvesFields",
    "BidCurvesTextFields",
]


BidCurvesTextFields = Literal["reserve_object"]
BidCurvesFields = Literal["reserve_object", "qty"]

_BIDCURVES_PROPERTIES_BY_FIELD = {
    "reserve_object": "ReserveObject",
    "qty": "Qty",
}


class BidCurves(DomainModel):
    space: str = "power-ops"
    reserve_object: Optional[str] = Field(None, alias="ReserveObject")
    qty: Optional[list[float]] = Field(None, alias="Qty")

    def as_apply(self) -> BidCurvesApply:
        return BidCurvesApply(
            external_id=self.external_id,
            reserve_object=self.reserve_object,
            qty=self.qty,
        )


class BidCurvesApply(DomainModelApply):
    space: str = "power-ops"
    reserve_object: Optional[str] = Field(None, alias="ReserveObject")
    qty: Optional[list[float]] = Field(None, alias="Qty")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.reserve_object is not None:
            properties["ReserveObject"] = self.reserve_object
        if self.qty is not None:
            properties["Qty"] = self.qty
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BidCurves"),
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


class BidCurvesList(TypeList[BidCurves]):
    _NODE = BidCurves

    def as_apply(self) -> BidCurvesApplyList:
        return BidCurvesApplyList([node.as_apply() for node in self.data])


class BidCurvesApplyList(TypeApplyList[BidCurvesApply]):
    _NODE = BidCurvesApply
