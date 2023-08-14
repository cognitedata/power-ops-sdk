from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["WatercourseShop", "WatercourseShopApply", "WatercourseShopList"]


class WatercourseShop(DomainModel):
    space: ClassVar[str] = "power-ops"
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")


class WatercourseShopApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    penalty_limit: Optional[float] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "WatercourseShop"),
            properties={
                "penaltyLimit": self.penalty_limit,
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


class WatercourseShopList(TypeList[WatercourseShop]):
    _NODE = WatercourseShop
