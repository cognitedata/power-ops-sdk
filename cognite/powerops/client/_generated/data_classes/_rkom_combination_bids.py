from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

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
        properties = {}
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.name is not None:
            properties["name"] = self.name
        if self.rkom_bid_configs is not None:
            properties["rkomBidConfigs"] = self.rkom_bid_configs
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "RKOMCombinationBid"),
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


class RKOMCombinationBidList(TypeList[RKOMCombinationBid]):
    _NODE = RKOMCombinationBid
