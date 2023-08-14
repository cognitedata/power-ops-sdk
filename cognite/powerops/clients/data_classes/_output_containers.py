from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._output_mappings import OutputMappingApply

__all__ = ["OutputContainer", "OutputContainerApply", "OutputContainerList"]


class OutputContainer(DomainModel):
    space: ClassVar[str] = "power-ops"
    mappings: list[str] = []
    name: Optional[str] = None
    shop_type: Optional[str] = Field(None, alias="shopType")
    watercourse: Optional[str] = None


class OutputContainerApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    mappings: list[Union["OutputMappingApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None
    shop_type: Optional[str] = None
    watercourse: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "OutputContainer"),
            properties={
                "name": self.name,
                "shopType": self.shop_type,
                "watercourse": self.watercourse,
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
        cache.add(self.external_id)

        for mapping in self.mappings:
            edge = self._create_mapping_edge(mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mapping, DomainModelApply):
                instances = mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_mapping_edge(self, mapping: Union[str, "OutputMappingApply"]) -> dm.EdgeApply:
        if isinstance(mapping, str):
            end_node_ext_id = mapping
        elif isinstance(mapping, DomainModelApply):
            end_node_ext_id = mapping.external_id
        else:
            raise TypeError(f"Expected str or OutputMappingApply, got {type(mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "OutputContainer.mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class OutputContainerList(TypeList[OutputContainer]):
    _NODE = OutputContainer
