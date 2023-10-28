from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._periods import PeriodsApply

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
    reserve_object: Optional[list[str]] = Field(None, alias="ReserveObject")
    qty: Optional[list[float]] = Field(None, alias="Qty")
    parent: Optional[list[str]] = None

    def as_apply(self) -> BidCurvesApply:
        return BidCurvesApply(
            external_id=self.external_id,
            reserve_object=self.reserve_object,
            qty=self.qty,
            parent=self.parent,
        )


class BidCurvesApply(DomainModelApply):
    space: str = "power-ops"
    reserve_object: Optional[list[str]] = Field(None, alias="ReserveObject")
    qty: Optional[list[float]] = Field(None, alias="Qty")
    parent: Union[list[PeriodsApply], list[str], None] = Field(default=None, repr=False)

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

    def _create_parent_edge(self, parent: Union[str, PeriodsApply]) -> dm.EdgeApply:
        if isinstance(parent, str):
            end_node_ext_id = parent
        elif isinstance(parent, DomainModelApply):
            end_node_ext_id = parent.external_id
        else:
            raise TypeError(f"Expected str or PeriodsApply, got {type(parent)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "BidCurves.parent"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class BidCurvesList(TypeList[BidCurves]):
    _NODE = BidCurves

    def as_apply(self) -> BidCurvesApplyList:
        return BidCurvesApplyList([node.as_apply() for node in self.data])


class BidCurvesApplyList(TypeApplyList[BidCurvesApply]):
    _NODE = BidCurvesApply
