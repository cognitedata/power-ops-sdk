from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._input_time_series_mappings import InputTimeSeriesMappingApply

__all__ = ["IncrementalMapping", "IncrementalMappingApply", "IncrementalMappingList"]


class IncrementalMapping(DomainModel):
    space: ClassVar[str] = "power-ops"
    mappings: list[str] = []
    name: Optional[str] = None


class IncrementalMappingApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    mappings: list[Union["InputTimeSeriesMappingApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "IncrementalMapping"),
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

        for mapping in self.mappings:
            edge = self._create_mapping_edge(mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mapping, DomainModelApply):
                instances = mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_mapping_edge(self, mapping: Union[str, "InputTimeSeriesMappingApply"]) -> dm.EdgeApply:
        if isinstance(mapping, str):
            end_node_ext_id = mapping
        elif isinstance(mapping, DomainModelApply):
            end_node_ext_id = mapping.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "IncrementalMapping.mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class IncrementalMappingList(TypeList[IncrementalMapping]):
    _NODE = IncrementalMapping
