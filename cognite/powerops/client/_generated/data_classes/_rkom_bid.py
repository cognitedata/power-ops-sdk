from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._date_transformation import DateTransformationApply
    from ._reserve_scenario import ReserveScenarioApply
    from ._rkom_market import RKOMMarketApply
    from ._scenario_mapping import ScenarioMappingApply

__all__ = ["RKOMBid", "RKOMBidApply", "RKOMBidList", "RKOMBidApplyList", "RKOMBidFields", "RKOMBidTextFields"]


RKOMBidTextFields = Literal["name", "method", "watercourse"]
RKOMBidFields = Literal["name", "method", "minimum_price", "price_premium", "watercourse"]

_RKOMBID_PROPERTIES_BY_FIELD = {
    "name": "name",
    "method": "method",
    "minimum_price": "minimumPrice",
    "price_premium": "pricePremium",
    "watercourse": "watercourse",
}


class RKOMBid(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Optional[str] = None
    method: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    price_premium: Optional[float] = Field(None, alias="pricePremium")
    watercourse: Optional[str] = None
    date: Optional[list[str]] = None
    price_scenarios: Optional[list[str]] = Field(None, alias="priceScenarios")
    reserve_scenarios: Optional[list[str]] = Field(None, alias="reserveScenarios")

    def as_apply(self) -> RKOMBidApply:
        return RKOMBidApply(
            external_id=self.external_id,
            name=self.name,
            market=self.market,
            method=self.method,
            minimum_price=self.minimum_price,
            price_premium=self.price_premium,
            watercourse=self.watercourse,
            date=self.date,
            price_scenarios=self.price_scenarios,
            reserve_scenarios=self.reserve_scenarios,
        )


class RKOMBidApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Union[RKOMMarketApply, str, None] = Field(None, repr=False)
    method: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    price_premium: Optional[float] = Field(None, alias="pricePremium")
    watercourse: Optional[str] = None
    date: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)
    price_scenarios: Union[list[ScenarioMappingApply], list[str], None] = Field(
        default=None, repr=False, alias="priceScenarios"
    )
    reserve_scenarios: Union[list[ReserveScenarioApply], list[str], None] = Field(
        default=None, repr=False, alias="reserveScenarios"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.market is not None:
            properties["market"] = {
                "space": "power-ops",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.method is not None:
            properties["method"] = self.method
        if self.minimum_price is not None:
            properties["minimumPrice"] = self.minimum_price
        if self.price_premium is not None:
            properties["pricePremium"] = self.price_premium
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "RKOMBid"),
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

        for date in self.date or []:
            edge = self._create_date_edge(date)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(date, DomainModelApply):
                instances = date._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for price_scenario in self.price_scenarios or []:
            edge = self._create_price_scenario_edge(price_scenario)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(price_scenario, DomainModelApply):
                instances = price_scenario._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for reserve_scenario in self.reserve_scenarios or []:
            edge = self._create_reserve_scenario_edge(reserve_scenario)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(reserve_scenario, DomainModelApply):
                instances = reserve_scenario._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_date_edge(self, date: Union[str, DateTransformationApply]) -> dm.EdgeApply:
        if isinstance(date, str):
            end_node_ext_id = date
        elif isinstance(date, DomainModelApply):
            end_node_ext_id = date.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(date)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMBid.date"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_price_scenario_edge(self, price_scenario: Union[str, ScenarioMappingApply]) -> dm.EdgeApply:
        if isinstance(price_scenario, str):
            end_node_ext_id = price_scenario
        elif isinstance(price_scenario, DomainModelApply):
            end_node_ext_id = price_scenario.external_id
        else:
            raise TypeError(f"Expected str or ScenarioMappingApply, got {type(price_scenario)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMBid.priceScenarios"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_reserve_scenario_edge(self, reserve_scenario: Union[str, ReserveScenarioApply]) -> dm.EdgeApply:
        if isinstance(reserve_scenario, str):
            end_node_ext_id = reserve_scenario
        elif isinstance(reserve_scenario, DomainModelApply):
            end_node_ext_id = reserve_scenario.external_id
        else:
            raise TypeError(f"Expected str or ReserveScenarioApply, got {type(reserve_scenario)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMBid.reserveScenarios"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMBidList(TypeList[RKOMBid]):
    _NODE = RKOMBid

    def as_apply(self) -> RKOMBidApplyList:
        return RKOMBidApplyList([node.as_apply() for node in self.data])


class RKOMBidApplyList(TypeApplyList[RKOMBidApply]):
    _NODE = RKOMBidApply
