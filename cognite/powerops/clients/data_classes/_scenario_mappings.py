from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._input_time_series_mappings import InputTimeSeriesMappingApply

__all__ = ["ScenarioMapping", "ScenarioMappingApply", "ScenarioMappingList"]


class ScenarioMapping(DomainModel):
    space: ClassVar[str] = "power-ops"
    mapping_override: list[str] = Field([], alias="mappingOverride")
    name: Optional[str] = None


class ScenarioMappingApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    mapping_override: list[Union["InputTimeSeriesMappingApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ScenarioMapping"),
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

        for mapping_override in self.mapping_override:
            edge = self._create_mapping_override_edge(mapping_override)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mapping_override, DomainModelApply):
                instances = mapping_override._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_mapping_override_edge(
        self, mapping_override: Union[str, "InputTimeSeriesMappingApply"]
    ) -> dm.EdgeApply:
        if isinstance(mapping_override, str):
            end_node_ext_id = mapping_override
        elif isinstance(mapping_override, DomainModelApply):
            end_node_ext_id = mapping_override.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(mapping_override)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ScenarioMapping.mappingOverride"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ScenarioMappingList(TypeList[ScenarioMapping]):
    _NODE = ScenarioMapping
