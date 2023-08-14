from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["RKOMCombinationBid", "RKOMCombinationBidApply", "RKOMCombinationBidList"]


class RKOMCombinationBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    name: Optional[str] = None
    rkom_bid_configs: list[str] = Field([], alias="rkomBidConfigs")


class RKOMCombinationBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    name: Optional[str] = None
    rkom_bid_configs: list[str] = []

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMCombinationBid"),
            properties={
                "auction": self.auction,
                "name": self.name,
                "rkomBidConfigs": self.rkom_bid_configs,
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


class RKOMCombinationBidList(TypeList[RKOMCombinationBid]):
    _NODE = RKOMCombinationBid
