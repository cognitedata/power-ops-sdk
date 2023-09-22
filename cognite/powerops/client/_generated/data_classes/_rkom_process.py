from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._rkom_bids import RKOMBidApply
    from cognite.powerops.client._generated.data_classes._scenario_mappings import ScenarioMappingApply
    from cognite.powerops.client._generated.data_classes._shop_transformations import ShopTransformationApply

__all__ = ["RKOMProces", "RKOMProcesApply", "RKOMProcesList"]


class RKOMProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    incremental_mappings: list[str] = []
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = Field([], alias="processEvents")
    shop: Optional[str] = None
    timezone: Optional[str] = None


class RKOMProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[RKOMBidApply, str]] = Field(None, repr=False)
    incremental_mappings: list[Union[ScenarioMappingApply, str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None
    plants: list[str] = []
    process_events: list[str] = []
    shop: Optional[Union[ShopTransformationApply, str]] = Field(None, repr=False)
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.bid is not None:
            properties["bid"] = {
                "space": "power-ops",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if self.plants is not None:
            properties["plants"] = self.plants
        if self.process_events is not None:
            properties["processEvents"] = self.process_events
        if self.shop is not None:
            properties["shop"] = {
                "space": "power-ops",
                "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
            }
        if self.timezone is not None:
            properties["timezone"] = self.timezone
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

        for incremental_mapping in self.incremental_mappings:
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


class RKOMProcesList(TypeList[RKOMProces]):
    _NODE = RKOMProces
