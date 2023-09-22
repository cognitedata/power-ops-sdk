from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._date_transformations import DateTransformationApply
    from cognite.powerops.client._generated.data_classes._reserve_scenarios import ReserveScenarioApply
    from cognite.powerops.client._generated.data_classes._rkom_markets import RKOMMarketApply
    from cognite.powerops.client._generated.data_classes._scenario_mappings import ScenarioMappingApply

__all__ = ["RKOMBid", "RKOMBidApply", "RKOMBidList"]


class RKOMBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    date: list[str] = []
    market: Optional[str] = None
    method: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")
    price_scenarios: list[str] = Field([], alias="priceScenarios")
    reserve_scenarios: list[str] = Field([], alias="reserveScenarios")
    watercourse: Optional[str] = None


class RKOMBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    date: list[Union[DateTransformationApply, str]] = Field(default_factory=list, repr=False)
    market: Optional[Union[RKOMMarketApply, str]] = Field(None, repr=False)
    method: Optional[str] = None
    minimum_price: Optional[float] = None
    name: Optional[str] = None
    price_premium: Optional[float] = None
    price_scenarios: list[Union[ScenarioMappingApply, str]] = Field(default_factory=list, repr=False)
    reserve_scenarios: list[Union[ReserveScenarioApply, str]] = Field(default_factory=list, repr=False)
    watercourse: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.market is not None:
            properties["market"] = {
                "space": "power-ops",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.method is not None:
            properties["method"] = self.method
        if self.minimum_price is not None:
            properties["minimumPrice"] = self.minimum_price
        if self.name is not None:
            properties["name"] = self.name
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

        for date in self.date:
            edge = self._create_date_edge(date)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(date, DomainModelApply):
                instances = date._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for price_scenario in self.price_scenarios:
            edge = self._create_price_scenario_edge(price_scenario)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(price_scenario, DomainModelApply):
                instances = price_scenario._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for reserve_scenario in self.reserve_scenarios:
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
