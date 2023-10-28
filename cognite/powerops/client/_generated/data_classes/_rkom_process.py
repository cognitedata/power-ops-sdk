from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._rkom_bid import RKOMBidApply
    from ._scenario_mapping import ScenarioMappingApply
    from ._shop_transformation import ShopTransformationApply

__all__ = [
    "RKOMProcess",
    "RKOMProcessApply",
    "RKOMProcessList",
    "RKOMProcessApplyList",
    "RKOMProcessFields",
    "RKOMProcessTextFields",
]


RKOMProcessTextFields = Literal["name", "process_events", "timezone", "plants"]
RKOMProcessFields = Literal["name", "process_events", "timezone", "plants"]

_RKOMPROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
    "process_events": "processEvents",
    "timezone": "timezone",
    "plants": "plants",
}


class RKOMProcess(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    bid: Optional[str] = None
    shop: Optional[str] = None
    process_events: Optional[list[str]] = Field(None, alias="processEvents")
    timezone: Optional[str] = None
    plants: Optional[list[str]] = None
    incremental_mappings: Optional[list[str]] = None

    def as_apply(self) -> RKOMProcessApply:
        return RKOMProcessApply(
            external_id=self.external_id,
            name=self.name,
            bid=self.bid,
            shop=self.shop,
            process_events=self.process_events,
            timezone=self.timezone,
            plants=self.plants,
            incremental_mappings=self.incremental_mappings,
        )


class RKOMProcessApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    bid: Union[RKOMBidApply, str, None] = Field(None, repr=False)
    shop: Union[ShopTransformationApply, str, None] = Field(None, repr=False)
    process_events: Optional[list[str]] = Field(None, alias="processEvents")
    timezone: Optional[str] = None
    plants: Optional[list[str]] = None
    incremental_mappings: Union[list[ScenarioMappingApply], list[str], None] = Field(default=None, repr=False)

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
        if self.process_events is not None:
            properties["processEvents"] = self.process_events
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if self.plants is not None:
            properties["plants"] = self.plants
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "RKOMProcess"),
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
            type=dm.DirectRelationReference("power-ops", "RKOMProcess.incremental_mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMProcessList(TypeList[RKOMProcess]):
    _NODE = RKOMProcess

    def as_apply(self) -> RKOMProcessApplyList:
        return RKOMProcessApplyList([node.as_apply() for node in self.data])


class RKOMProcessApplyList(TypeApplyList[RKOMProcessApply]):
    _NODE = RKOMProcessApply
