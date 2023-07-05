from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._date_transformations import DateTransformationApply
    from ._markets import MarketApply

__all__ = ["RKOMBid", "RKOMBidApply", "RKOMBidList"]


class RKOMBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    date: list[str] = []
    market: Optional[str] = None
    method: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")
    price_scenarios: list[str] = Field([], alias="priceScenarios")
    product: Optional[str] = None
    reserve_scenarios: list[str] = Field([], alias="reserveScenarios")
    watercourse: Optional[str] = None


class RKOMBidApply(CircularModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    block: Optional[str] = None
    date: list[Union[str, "DateTransformationApply"]] = []
    market: Optional[Union[str, "MarketApply"]] = None
    method: Optional[str] = None
    minimum_price: Optional[float] = None
    name: Optional[str] = None
    price_premium: Optional[float] = None
    price_scenarios: list[str] = []
    product: Optional[str] = None
    reserve_scenarios: list[str] = []
    watercourse: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMBid"),
            properties={
                "auction": self.auction,
                "block": self.block,
                "method": self.method,
                "minimumPrice": self.minimum_price,
                "pricePremium": self.price_premium,
                "priceScenarios": self.price_scenarios,
                "product": self.product,
                "reserveScenarios": self.reserve_scenarios,
                "watercourse": self.watercourse,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Bid"),
            properties={
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

        for date in self.date:
            edge = self._create_date_edge(date)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(date, DomainModelApply):
                instances = date._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_date_edge(self, date: Union[str, "DateTransformationApply"]) -> dm.EdgeApply:
        if isinstance(date, str):
            end_node_ext_id = date
        elif isinstance(date, DomainModelApply):
            end_node_ext_id = date.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(date)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Bid.date"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMBidList(TypeList[RKOMBid]):
    _NODE = RKOMBid
