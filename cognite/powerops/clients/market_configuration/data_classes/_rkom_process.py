from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._rkom_bids import RKOMBidApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["RKOMProces", "RKOMProcesApply", "RKOMProcesList"]


class RKOMProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    incremental_mapping: list[str] = Field([], alias="incrementalMapping")
    name: Optional[str] = None
    process_events: list[str] = Field([], alias="processEvents")
    shop: Optional[str] = None
    timezone: Optional[str] = None


class RKOMProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[str, "RKOMBidApply"]] = Field(None, repr=False)
    incremental_mapping: list[str] = []
    name: Optional[str] = None
    process_events: list[str] = []
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMProcess"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "incrementalMapping": self.incremental_mapping,
                "name": self.name,
                "processEvents": self.process_events,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
                "timezone": self.timezone,
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

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class RKOMProcesList(TypeList[RKOMProces]):
    _NODE = RKOMProces
