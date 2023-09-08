from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

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

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.start_of_week is not None:
            properties["startOfWeek"] = self.start_of_week
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "RKOMMarket"),
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


class RKOMMarketList(TypeList[RKOMMarket]):
    _NODE = RKOMMarket
