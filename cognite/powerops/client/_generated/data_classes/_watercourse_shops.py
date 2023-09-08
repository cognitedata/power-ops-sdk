from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

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
        properties = {}
        if self.penalty_limit is not None:
            properties["penaltyLimit"] = self.penalty_limit
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "WatercourseShop"),
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


class WatercourseShopList(TypeList[WatercourseShop]):
    _NODE = WatercourseShop
