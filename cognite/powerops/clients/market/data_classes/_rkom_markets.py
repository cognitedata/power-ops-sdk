from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["RKOMMarket", "RKOMMarketApply", "RKOMMarketList"]


class RKOMMarket(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    start_of_week: Optional[int] = Field(None, alias="startOfWeek")
    timezone: Optional[str] = None


class RKOMMarketApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    start_of_week: Optional[int] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Market"),
            properties={
                "name": self.name,
                "timezone": self.timezone,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMMarket"),
            properties={
                "startOfWeek": self.start_of_week,
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


class RKOMMarketList(TypeList[RKOMMarket]):
    _NODE = RKOMMarket
