from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._date_transformations import DateTransformationApply
    from cognite.powerops.clients.data_classes._nord_pool_markets import NordPoolMarketApply
    from cognite.powerops.clients.data_classes._scenario_mappings import ScenarioMappingApply

__all__ = ["DayAheadBid", "DayAheadBidApply", "DayAheadBidList"]


class DayAheadBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid_matrix_generator_config_external_id: Optional[str] = Field(None, alias="bidMatrixGeneratorConfigExternalId")
    bid_process_configuration_name: Optional[str] = Field(None, alias="bidProcessConfigurationName")
    date: list[str] = []
    is_default_config_for_price_area: Optional[bool] = Field(None, alias="isDefaultConfigForPriceArea")
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    market: Optional[str] = None
    name: Optional[str] = None
    no_shop: Optional[bool] = Field(None, alias="noShop")
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_scenarios: list[str] = Field([], alias="priceScenarios")


class DayAheadBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid_matrix_generator_config_external_id: Optional[str] = None
    bid_process_configuration_name: Optional[str] = None
    date: list[Union["DateTransformationApply", str]] = Field(default_factory=list, repr=False)
    is_default_config_for_price_area: Optional[bool] = None
    main_scenario: Optional[str] = None
    market: Optional[Union["NordPoolMarketApply", str]] = Field(None, repr=False)
    name: Optional[str] = None
    no_shop: Optional[bool] = None
    price_area: Optional[str] = None
    price_scenarios: list[Union["ScenarioMappingApply", str]] = Field(default_factory=list, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "DayAheadBid"),
            properties={
                "bidMatrixGeneratorConfigExternalId": self.bid_matrix_generator_config_external_id,
                "bidProcessConfigurationName": self.bid_process_configuration_name,
                "isDefaultConfigForPriceArea": self.is_default_config_for_price_area,
                "mainScenario": self.main_scenario,
                "market": {
                    "space": "power-ops",
                    "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
                },
                "name": self.name,
                "noShop": self.no_shop,
                "priceArea": self.price_area,
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

        for price_scenario in self.price_scenarios:
            edge = self._create_price_scenario_edge(price_scenario)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(price_scenario, DomainModelApply):
                instances = price_scenario._to_instances_apply(cache)
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
            type=dm.DirectRelationReference("power-ops", "DayAheadBid.date"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_price_scenario_edge(self, price_scenario: Union[str, "ScenarioMappingApply"]) -> dm.EdgeApply:
        if isinstance(price_scenario, str):
            end_node_ext_id = price_scenario
        elif isinstance(price_scenario, DomainModelApply):
            end_node_ext_id = price_scenario.external_id
        else:
            raise TypeError(f"Expected str or ScenarioMappingApply, got {type(price_scenario)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "DayAheadBid.priceScenarios"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class DayAheadBidList(TypeList[DayAheadBid]):
    _NODE = DayAheadBid
