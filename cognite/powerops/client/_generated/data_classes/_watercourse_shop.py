from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "WatercourseShop",
    "WatercourseShopApply",
    "WatercourseShopList",
    "WatercourseShopApplyList",
    "WatercourseShopFields",
]
WatercourseShopFields = Literal["penalty_limit"]

_WATERCOURSESHOP_PROPERTIES_BY_FIELD = {
    "penalty_limit": "penaltyLimit",
}


class WatercourseShop(DomainModel):
    space: str = "power-ops"
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")

    def as_apply(self) -> WatercourseShopApply:
        return WatercourseShopApply(
            external_id=self.external_id,
            penalty_limit=self.penalty_limit,
        )


class WatercourseShopApply(DomainModelApply):
    space: str = "power-ops"
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")

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

    def as_apply(self) -> WatercourseShopApplyList:
        return WatercourseShopApplyList([node.as_apply() for node in self.data])


class WatercourseShopApplyList(TypeApplyList[WatercourseShopApply]):
    _NODE = WatercourseShopApply
