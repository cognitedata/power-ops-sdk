from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._bids import BidApply
    from cognite.powerops.clients.data_classes._scenario_mappings import ScenarioMappingApply
    from cognite.powerops.clients.data_classes._shop_transformations import ShopTransformationApply

__all__ = ["RKOMProces", "RKOMProcesApply", "RKOMProcesList"]


class RKOMProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = Field([], alias="processEvents")
    scenario_mappings: list[str] = []
    shop: Optional[str] = None
    timezone: Optional[str] = None


class RKOMProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union["BidApply", str]] = Field(None, repr=False)
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = []
    scenario_mappings: list[Union["ScenarioMappingApply", str]] = Field(default_factory=list, repr=False)
    shop: Optional[Union["ShopTransformationApply", str]] = Field(None, repr=False)
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Process"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "name": self.name,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMProcess"),
            properties={
                "plants": self.plants,
                "processEvents": self.process_events,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
                "timezone": self.timezone,
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

        for scenario_mapping in self.scenario_mappings:
            edge = self._create_scenario_mapping_edge(scenario_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(scenario_mapping, DomainModelApply):
                instances = scenario_mapping._to_instances_apply(cache)
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

        return InstancesApply(nodes, edges)

    def _create_scenario_mapping_edge(self, scenario_mapping: Union[str, "ScenarioMappingApply"]) -> dm.EdgeApply:
        if isinstance(scenario_mapping, str):
            end_node_ext_id = scenario_mapping
        elif isinstance(scenario_mapping, DomainModelApply):
            end_node_ext_id = scenario_mapping.external_id
        else:
            raise TypeError(f"Expected str or ScenarioMappingApply, got {type(scenario_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMProcess.scenario_mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMProcesList(TypeList[RKOMProces]):
    _NODE = RKOMProces
