from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._date_transformation import DateTransformationApply
    from ._nord_pool_market import NordPoolMarketApply
    from ._scenario_mapping import ScenarioMappingApply

__all__ = [
    "DayAheadBid",
    "DayAheadBidApply",
    "DayAheadBidList",
    "DayAheadBidApplyList",
    "DayAheadBidFields",
    "DayAheadBidTextFields",
]


DayAheadBidTextFields = Literal[
    "name",
    "main_scenario",
    "price_area",
    "watercourse",
    "bid_process_configuration_name",
    "bid_matrix_generator_config_external_id",
]
DayAheadBidFields = Literal[
    "name",
    "is_default_config_for_price_area",
    "main_scenario",
    "price_area",
    "watercourse",
    "no_shop",
    "bid_process_configuration_name",
    "bid_matrix_generator_config_external_id",
]

_DAYAHEADBID_PROPERTIES_BY_FIELD = {
    "name": "name",
    "is_default_config_for_price_area": "isDefaultConfigForPriceArea",
    "main_scenario": "mainScenario",
    "price_area": "priceArea",
    "watercourse": "watercourse",
    "no_shop": "noShop",
    "bid_process_configuration_name": "bidProcessConfigurationName",
    "bid_matrix_generator_config_external_id": "bidMatrixGeneratorConfigExternalId",
}


class DayAheadBid(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Optional[str] = None
    is_default_config_for_price_area: Optional[bool] = Field(None, alias="isDefaultConfigForPriceArea")
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    price_area: Optional[str] = Field(None, alias="priceArea")
    watercourse: Optional[str] = None
    no_shop: Optional[bool] = Field(None, alias="noShop")
    bid_process_configuration_name: Optional[str] = Field(None, alias="bidProcessConfigurationName")
    bid_matrix_generator_config_external_id: Optional[str] = Field(None, alias="bidMatrixGeneratorConfigExternalId")
    date: Optional[list[str]] = None
    price_scenarios: Optional[list[str]] = Field(None, alias="priceScenarios")

    def as_apply(self) -> DayAheadBidApply:
        return DayAheadBidApply(
            external_id=self.external_id,
            name=self.name,
            market=self.market,
            is_default_config_for_price_area=self.is_default_config_for_price_area,
            main_scenario=self.main_scenario,
            price_area=self.price_area,
            watercourse=self.watercourse,
            no_shop=self.no_shop,
            bid_process_configuration_name=self.bid_process_configuration_name,
            bid_matrix_generator_config_external_id=self.bid_matrix_generator_config_external_id,
            date=self.date,
            price_scenarios=self.price_scenarios,
        )


class DayAheadBidApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Union[NordPoolMarketApply, str, None] = Field(None, repr=False)
    is_default_config_for_price_area: Optional[bool] = Field(None, alias="isDefaultConfigForPriceArea")
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    price_area: Optional[str] = Field(None, alias="priceArea")
    watercourse: Optional[str] = None
    no_shop: Optional[bool] = Field(None, alias="noShop")
    bid_process_configuration_name: Optional[str] = Field(None, alias="bidProcessConfigurationName")
    bid_matrix_generator_config_external_id: Optional[str] = Field(None, alias="bidMatrixGeneratorConfigExternalId")
    date: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)
    price_scenarios: Union[list[ScenarioMappingApply], list[str], None] = Field(
        default=None, repr=False, alias="priceScenarios"
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
        if self.is_default_config_for_price_area is not None:
            properties["isDefaultConfigForPriceArea"] = self.is_default_config_for_price_area
        if self.main_scenario is not None:
            properties["mainScenario"] = self.main_scenario
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if self.no_shop is not None:
            properties["noShop"] = self.no_shop
        if self.bid_process_configuration_name is not None:
            properties["bidProcessConfigurationName"] = self.bid_process_configuration_name
        if self.bid_matrix_generator_config_external_id is not None:
            properties["bidMatrixGeneratorConfigExternalId"] = self.bid_matrix_generator_config_external_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "DayAheadBid"),
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
            type=dm.DirectRelationReference("power-ops", "DayAheadBid.date"),
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
            type=dm.DirectRelationReference("power-ops", "DayAheadBid.priceScenarios"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class DayAheadBidList(TypeList[DayAheadBid]):
    _NODE = DayAheadBid

    def as_apply(self) -> DayAheadBidApplyList:
        return DayAheadBidApplyList([node.as_apply() for node in self.data])


class DayAheadBidApplyList(TypeApplyList[DayAheadBidApply]):
    _NODE = DayAheadBidApply
