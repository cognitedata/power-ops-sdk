from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["ReserveScenario", "ReserveScenarioApply", "ReserveScenarioList", "ReserveScenarioApplyList"]


class ReserveScenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    volume: Optional[int] = None
    auction: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None
    reserve_group: Optional[str] = Field(None, alias="reserveGroup")

    def as_apply(self) -> ReserveScenarioApply:
        return ReserveScenarioApply(
            external_id=self.external_id,
            volume=self.volume,
            auction=self.auction,
            product=self.product,
            block=self.block,
            reserve_group=self.reserve_group,
        )


class ReserveScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    volume: Optional[int] = None
    auction: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None
    reserve_group: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.volume is not None:
            properties["volume"] = self.volume
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.product is not None:
            properties["product"] = self.product
        if self.block is not None:
            properties["block"] = self.block
        if self.reserve_group is not None:
            properties["reserveGroup"] = self.reserve_group
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ReserveScenario"),
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


class ReserveScenarioList(TypeList[ReserveScenario]):
    _NODE = ReserveScenario

    def as_apply(self) -> ReserveScenarioApplyList:
        return ReserveScenarioApplyList([node.as_apply() for node in self.data])


class ReserveScenarioApplyList(TypeApplyList[ReserveScenarioApply]):
    _NODE = ReserveScenarioApply
