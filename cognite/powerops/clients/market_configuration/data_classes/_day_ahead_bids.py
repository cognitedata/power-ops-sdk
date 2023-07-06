from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._date_transformations import DateTransformationApply
    from ._markets import MarketApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["DayAheadBid", "DayAheadBidApply", "DayAheadBidList"]


class DayAheadBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    date: list[str] = []
    is_default_config_for_price_area: Optional[bool] = Field(None, alias="isDefaultConfigForPriceArea")
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    market: Optional[str] = None
    name: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_scenarios: list[dict] = Field([], alias="priceScenarios")
    shop: Optional[str] = None


class DayAheadBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    date: list[Union[str, "DateTransformationApply"]] = Field(default_factory=lambda: [], repr=False)
    is_default_config_for_price_area: Optional[bool] = None
    main_scenario: Optional[str] = None
    market: Optional[Union[str, "MarketApply"]] = Field(None, repr=False)
    name: Optional[str] = None
    price_area: Optional[str] = None
    price_scenarios: list[dict] = []
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "DayAheadBid"),
            properties={
                "isDefaultConfigForPriceArea": self.is_default_config_for_price_area,
                "mainScenario": self.main_scenario,
                "priceArea": self.price_area,
                "priceScenarios": self.price_scenarios,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
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

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
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


class DayAheadBidList(TypeList[DayAheadBid]):
    _NODE = DayAheadBid
