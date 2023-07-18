from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._rkom_combination_bids import RKOMCombinationBidApply

__all__ = ["RKOMBidCombination", "RKOMBidCombinationApply", "RKOMBidCombinationList"]


class RKOMBidCombination(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid: Optional[str] = None
    name: Optional[str] = None


class RKOMBidCombinationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid: Optional[Union["RKOMCombinationBidApply", str]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMBidCombination"),
            properties={
                "auction": self.auction,
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "name": self.name,
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

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class RKOMBidCombinationList(TypeList[RKOMBidCombination]):
    _NODE = RKOMBidCombination
