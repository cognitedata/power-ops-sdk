from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["ReserveScenario", "ReserveScenarioApply", "ReserveScenarioList"]


class ReserveScenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    mip_plant: list[str] = Field([], alias="mipPlant")
    obligation: Optional[str] = None
    product: Optional[str] = None
    reserve_group: Optional[str] = Field(None, alias="reserveGroup")
    volumes: list[int] = []


class ReserveScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    mip_plant: list[str] = []
    obligation: Optional[str] = None
    product: Optional[str] = None
    reserve_group: Optional[str] = None
    volumes: list[int] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ReserveScenario"),
            properties={
                "auction": self.auction,
                "block": self.block,
                "mipPlant": self.mip_plant,
                "obligation": self.obligation,
                "product": self.product,
                "reserveGroup": self.reserve_group,
                "volumes": self.volumes,
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

        return InstancesApply(nodes, edges)


class ReserveScenarioList(TypeList[ReserveScenario]):
    _NODE = ReserveScenario
