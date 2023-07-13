from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._markets import MarketApply

__all__ = ["Bid", "BidApply", "BidList"]


class Bid(DomainModel):
    space: ClassVar[str] = "power-ops"
    date: Optional[date] = None
    market: Optional[str] = None
    name: Optional[str] = None


class BidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    date: Optional[date] = None
    market: Optional[Union[str, "MarketApply"]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Bid"),
            properties={
                "date": self.date,
                "market": {
                    "space": "power-ops",
                    "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
                },
                "name": self.name,
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

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class BidList(TypeList[Bid]):
    _NODE = Bid
