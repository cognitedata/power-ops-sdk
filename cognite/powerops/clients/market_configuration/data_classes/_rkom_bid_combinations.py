from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._rkom_process import RKOMProcesApply

__all__ = ["RKOMBidCombination", "RKOMBidCombinationApply", "RKOMBidCombinationList"]


class RKOMBidCombination(DomainModel):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid_configurations: list[str] = Field([], alias="bidConfigurations")
    name: Optional[str] = None


class RKOMBidCombinationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    auction: Optional[str] = None
    bid_configurations: list[Union[str, "RKOMProcesApply"]] = Field(default_factory=lambda: [], repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMBidCombination"),
            properties={
                "auction": self.auction,
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

        for bid_configuration in self.bid_configurations:
            edge = self._create_bid_configuration_edge(bid_configuration)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(bid_configuration, DomainModelApply):
                instances = bid_configuration._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_bid_configuration_edge(self, bid_configuration: Union[str, "RKOMProcesApply"]) -> dm.EdgeApply:
        if isinstance(bid_configuration, str):
            end_node_ext_id = bid_configuration
        elif isinstance(bid_configuration, DomainModelApply):
            end_node_ext_id = bid_configuration.external_id
        else:
            raise TypeError(f"Expected str or RKOMProcessApply, got {type(bid_configuration)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMBidCombination.bidConfigurations"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMBidCombinationList(TypeList[RKOMBidCombination]):
    _NODE = RKOMBidCombination
