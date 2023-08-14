from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

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
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ReserveScenario"),
            properties={
                "auction": self.auction,
                "block": self.block,
                "product": self.product,
                "reserveGroup": self.reserve_group,
                "volume": self.volume,
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


class ReserveScenarioList(TypeList[ReserveScenario]):
    _NODE = ReserveScenario
