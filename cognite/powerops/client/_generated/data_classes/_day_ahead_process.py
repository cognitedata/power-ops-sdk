from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._bid_matrix_generator import BidMatrixGeneratorApply
    from ._day_ahead_bid import DayAheadBidApply
    from ._scenario_mapping import ScenarioMappingApply
    from ._shop_transformation import ShopTransformationApply

__all__ = [
    "DayAheadProcess",
    "DayAheadProcessApply",
    "DayAheadProcessList",
    "DayAheadProcessApplyList",
    "DayAheadProcessFields",
    "DayAheadProcessTextFields",
]


DayAheadProcessTextFields = Literal["name"]
DayAheadProcessFields = Literal["name"]

_DAYAHEADPROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class DayAheadProcess(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    bid: Optional[str] = None
    shop: Optional[str] = None
    incremental_mappings: Optional[list[str]] = None
    bid_matrix_generator_config: Optional[list[str]] = Field(None, alias="bidMatrixGeneratorConfig")

    def as_apply(self) -> DayAheadProcessApply:
        return DayAheadProcessApply(
            external_id=self.external_id,
            name=self.name,
            bid=self.bid,
            shop=self.shop,
            incremental_mappings=self.incremental_mappings,
            bid_matrix_generator_config=self.bid_matrix_generator_config,
        )


class DayAheadProcessApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    bid: Union[DayAheadBidApply, str, None] = Field(None, repr=False)
    shop: Union[ShopTransformationApply, str, None] = Field(None, repr=False)
    incremental_mappings: Union[list[ScenarioMappingApply], list[str], None] = Field(default=None, repr=False)
    bid_matrix_generator_config: Union[list[BidMatrixGeneratorApply], list[str], None] = Field(
        default=None, repr=False, alias="bidMatrixGeneratorConfig"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.shop is not None:
            properties["shop"] = {
                "space": "power-ops",
                "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "DayAheadProcess"),
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

        for incremental_mapping in self.incremental_mappings or []:
            edge = self._create_incremental_mapping_edge(incremental_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(incremental_mapping, DomainModelApply):
                instances = incremental_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for bid_matrix_generator_config in self.bid_matrix_generator_config or []:
            edge = self._create_bid_matrix_generator_config_edge(bid_matrix_generator_config)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(bid_matrix_generator_config, DomainModelApply):
                instances = bid_matrix_generator_config._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_incremental_mapping_edge(self, incremental_mapping: Union[str, ScenarioMappingApply]) -> dm.EdgeApply:
        if isinstance(incremental_mapping, str):
            end_node_ext_id = incremental_mapping
        elif isinstance(incremental_mapping, DomainModelApply):
            end_node_ext_id = incremental_mapping.external_id
        else:
            raise TypeError(f"Expected str or ScenarioMappingApply, got {type(incremental_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "DayAheadProcess.incremental_mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_bid_matrix_generator_config_edge(
        self, bid_matrix_generator_config: Union[str, BidMatrixGeneratorApply]
    ) -> dm.EdgeApply:
        if isinstance(bid_matrix_generator_config, str):
            end_node_ext_id = bid_matrix_generator_config
        elif isinstance(bid_matrix_generator_config, DomainModelApply):
            end_node_ext_id = bid_matrix_generator_config.external_id
        else:
            raise TypeError(f"Expected str or BidMatrixGeneratorApply, got {type(bid_matrix_generator_config)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "DayAheadProcess.bidMatrixGeneratorConfig"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class DayAheadProcessList(TypeList[DayAheadProcess]):
    _NODE = DayAheadProcess

    def as_apply(self) -> DayAheadProcessApplyList:
        return DayAheadProcessApplyList([node.as_apply() for node in self.data])


class DayAheadProcessApplyList(TypeApplyList[DayAheadProcessApply]):
    _NODE = DayAheadProcessApply
