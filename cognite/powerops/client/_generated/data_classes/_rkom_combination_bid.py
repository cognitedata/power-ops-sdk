from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "RKOMCombinationBid",
    "RKOMCombinationBidApply",
    "RKOMCombinationBidList",
    "RKOMCombinationBidApplyList",
    "RKOMCombinationBidFields",
    "RKOMCombinationBidTextFields",
]


RKOMCombinationBidTextFields = Literal["name", "auction", "rkom_bid_configs"]
RKOMCombinationBidFields = Literal["name", "auction", "rkom_bid_configs"]

_RKOMCOMBINATIONBID_PROPERTIES_BY_FIELD = {
    "name": "name",
    "auction": "auction",
    "rkom_bid_configs": "rkomBidConfigs",
}


class RKOMCombinationBid(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    auction: Optional[str] = None
    rkom_bid_configs: Optional[list[str]] = Field(None, alias="rkomBidConfigs")

    def as_apply(self) -> RKOMCombinationBidApply:
        return RKOMCombinationBidApply(
            external_id=self.external_id,
            name=self.name,
            auction=self.auction,
            rkom_bid_configs=self.rkom_bid_configs,
        )


class RKOMCombinationBidApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    auction: Optional[str] = None
    rkom_bid_configs: Optional[list[str]] = Field(None, alias="rkomBidConfigs")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.auction is not None:
            properties["auction"] = self.auction
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

    def as_apply(self) -> RKOMCombinationBidApplyList:
        return RKOMCombinationBidApplyList([node.as_apply() for node in self.data])


class RKOMCombinationBidApplyList(TypeApplyList[RKOMCombinationBidApply]):
    _NODE = RKOMCombinationBidApply
