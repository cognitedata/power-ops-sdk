from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._rkom_combination_bids import RKOMCombinationBidApply

__all__ = ["RKOMBidCombination", "RKOMBidCombinationApply", "RKOMBidCombinationList"]


class RKOMBidCombination(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid: Optional[str] = None
    name: Optional[str] = None


class RKOMBidCombinationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid: Optional[Union[RKOMCombinationBidApply, str]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
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
