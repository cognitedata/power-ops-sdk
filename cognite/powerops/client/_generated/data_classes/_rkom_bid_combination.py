from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._rkom_combination_bid import RKOMCombinationBidApply

__all__ = [
    "RKOMBidCombination",
    "RKOMBidCombinationApply",
    "RKOMBidCombinationList",
    "RKOMBidCombinationApplyList",
    "RKOMBidCombinationFields",
    "RKOMBidCombinationTextFields",
]


RKOMBidCombinationTextFields = Literal["name", "auction"]
RKOMBidCombinationFields = Literal["name", "auction"]

_RKOMBIDCOMBINATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "auction": "auction",
}


class RKOMBidCombination(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    auction: Optional[str] = None
    bid: Optional[str] = None

    def as_apply(self) -> RKOMBidCombinationApply:
        return RKOMBidCombinationApply(
            external_id=self.external_id,
            name=self.name,
            auction=self.auction,
            bid=self.bid,
        )


class RKOMBidCombinationApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    auction: Optional[str] = None
    bid: Union[RKOMCombinationBidApply, str, None] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "RKOMBidCombination"),
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

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class RKOMBidCombinationList(TypeList[RKOMBidCombination]):
    _NODE = RKOMBidCombination

    def as_apply(self) -> RKOMBidCombinationApplyList:
        return RKOMBidCombinationApplyList([node.as_apply() for node in self.data])


class RKOMBidCombinationApplyList(TypeApplyList[RKOMBidCombinationApply]):
    _NODE = RKOMBidCombinationApply
