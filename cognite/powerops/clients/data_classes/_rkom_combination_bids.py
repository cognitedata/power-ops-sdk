from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._scenario_mappings import ScenarioMappingApply

__all__ = ["RKOMCombinationBid", "RKOMCombinationBidApply", "RKOMCombinationBidList"]


class RKOMCombinationBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    incremental_mapping: list[str] = []
    name: Optional[str] = None
    rkom_bid_configs: list[str] = Field([], alias="rkomBidConfigs")


class RKOMCombinationBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    incremental_mapping: list[Union["ScenarioMappingApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None
    rkom_bid_configs: list[str] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMCombinationBid"),
            properties={
                "auction": self.auction,
                "name": self.name,
                "rkomBidConfigs": self.rkom_bid_configs,
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

        for incremental_mapping in self.incremental_mapping:
            edge = self._create_incremental_mapping_edge(incremental_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(incremental_mapping, DomainModelApply):
                instances = incremental_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_incremental_mapping_edge(self, incremental_mapping: Union[str, "ScenarioMappingApply"]) -> dm.EdgeApply:
        if isinstance(incremental_mapping, str):
            end_node_ext_id = incremental_mapping
        elif isinstance(incremental_mapping, DomainModelApply):
            end_node_ext_id = incremental_mapping.external_id
        else:
            raise TypeError(f"Expected str or ScenarioMappingApply, got {type(incremental_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMCombinationBid.incremental_mapping"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMCombinationBidList(TypeList[RKOMCombinationBid]):
    _NODE = RKOMCombinationBid
