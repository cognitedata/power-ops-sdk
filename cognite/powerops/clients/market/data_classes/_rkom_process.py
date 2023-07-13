from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._incremental_mappings import IncrementalMappingApply
    from ._rkom_bids import RKOMBidApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["RKOMProces", "RKOMProcesApply", "RKOMProcesList"]


class RKOMProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    incremental_mapping: list[str] = Field([], alias="incrementalMapping")
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = Field([], alias="processEvents")
    shop: Optional[str] = None
    timezone: Optional[str] = None


class RKOMProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[str, "RKOMBidApply"]] = Field(None, repr=False)
    incremental_mapping: list[Union[str, "IncrementalMappingApply"]] = Field(default_factory=lambda: [], repr=False)
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = []
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "RKOMProcess"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
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

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Process"),
            properties={
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

        for incremental_mapping in self.incremental_mapping:
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

        return InstancesApply(nodes, edges)

    def _create_incremental_mapping_edge(
        self, incremental_mapping: Union[str, "IncrementalMappingApply"]
    ) -> dm.EdgeApply:
        if isinstance(incremental_mapping, str):
            end_node_ext_id = incremental_mapping
        elif isinstance(incremental_mapping, DomainModelApply):
            end_node_ext_id = incremental_mapping.external_id
        else:
            raise TypeError(f"Expected str or IncrementalMappingApply, got {type(incremental_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "RKOMProcess.incrementalMapping"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class RKOMProcesList(TypeList[RKOMProces]):
    _NODE = RKOMProces
