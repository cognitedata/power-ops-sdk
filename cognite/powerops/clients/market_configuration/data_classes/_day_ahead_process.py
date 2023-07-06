from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._day_ahead_bids import DayAheadBidApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["DayAheadProces", "DayAheadProcesApply", "DayAheadProcesList"]


class DayAheadProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    incremental_mapping: list[str] = Field([], alias="incrementalMapping")
    name: Optional[str] = None
    shop: Optional[str] = None


class DayAheadProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[str, "DayAheadBidApply"]] = Field(None, repr=False)
    incremental_mapping: list[str] = []
    name: Optional[str] = None
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "DayAheadProcess"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "incrementalMapping": self.incremental_mapping,
                "name": self.name,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
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


class DayAheadProcesList(TypeList[DayAheadProces]):
    _NODE = DayAheadProces
