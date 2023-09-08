from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["ReserveScenario", "ReserveScenarioApply", "ReserveScenarioList"]


class ReserveScenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    product: Optional[str] = None
    reserve_group: Optional[str] = Field(None, alias="reserveGroup")
    volume: Optional[int] = None


class ReserveScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    product: Optional[str] = None
    reserve_group: Optional[str] = None
    volume: Optional[int] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.block is not None:
            properties["block"] = self.block
        if self.product is not None:
            properties["product"] = self.product
        if self.reserve_group is not None:
            properties["reserveGroup"] = self.reserve_group
        if self.volume is not None:
            properties["volume"] = self.volume
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
