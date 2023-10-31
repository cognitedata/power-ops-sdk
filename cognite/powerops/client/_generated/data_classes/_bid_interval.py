from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._bid_document_header import BidDocumentHeaderApply

__all__ = [
    "BidInterval",
    "BidIntervalApply",
    "BidIntervalList",
    "BidIntervalApplyList",
    "BidIntervalFields",
    "BidIntervalTextFields",
]


BidIntervalTextFields = Literal["start", "end"]
BidIntervalFields = Literal["start", "end"]

_BIDINTERVAL_PROPERTIES_BY_FIELD = {
    "start": "Start",
    "end": "End",
}


class BidInterval(DomainModel):
    space: str = "power-ops"
    start: Optional[list[str]] = Field(None, alias="Start")
    end: Optional[list[str]] = Field(None, alias="End")
    parent: Optional[list[str]] = None

    def as_apply(self) -> BidIntervalApply:
        return BidIntervalApply(
            external_id=self.external_id,
            start=self.start,
            end=self.end,
            parent=self.parent,
        )


class BidIntervalApply(DomainModelApply):
    space: str = "power-ops"
    start: Optional[list[str]] = Field(None, alias="Start")
    end: Optional[list[str]] = Field(None, alias="End")
    parent: Union[list[BidDocumentHeaderApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.start is not None:
            properties["Start"] = self.start
        if self.end is not None:
            properties["End"] = self.end
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BidInterval"),
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

    def _create_parent_edge(self, parent: Union[str, BidDocumentHeaderApply]) -> dm.EdgeApply:
        if isinstance(parent, str):
            end_node_ext_id = parent
        elif isinstance(parent, DomainModelApply):
            end_node_ext_id = parent.external_id
        else:
            raise TypeError(f"Expected str or BidDocumentHeaderApply, got {type(parent)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "BidInterval.parent"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class BidIntervalList(TypeList[BidInterval]):
    _NODE = BidInterval

    def as_apply(self) -> BidIntervalApplyList:
        return BidIntervalApplyList([node.as_apply() for node in self.data])


class BidIntervalApplyList(TypeApplyList[BidIntervalApply]):
    _NODE = BidIntervalApply
