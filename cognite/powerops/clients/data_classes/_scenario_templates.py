from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._input_time_series_mappings import InputTimeSeriesMappingApply
    from cognite.powerops.clients.data_classes._output_mappings import OutputMappingApply

__all__ = ["ScenarioTemplate", "ScenarioTemplateApply", "ScenarioTemplateList"]


class ScenarioTemplate(DomainModel):
    space: ClassVar[str] = "power-ops"
    base_mapping: list[str] = Field([], alias="baseMapping")
    model: Optional[str] = None
    output_definitions: list[str] = Field([], alias="outputDefinitions")
    shop_files: list[str] = Field([], alias="shopFiles")
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    template_version: Optional[str] = Field(None, alias="templateVersion")
    watercourse: Optional[str] = None


class ScenarioTemplateApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    base_mapping: list[Union["InputTimeSeriesMappingApply", str]] = Field(default_factory=list, repr=False)
    model: Optional[str] = None
    output_definitions: list[Union["OutputMappingApply", str]] = Field(default_factory=list, repr=False)
    shop_files: list[str] = []
    shop_version: Optional[str] = None
    template_version: Optional[str] = None
    watercourse: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ScenarioTemplate"),
            properties={
                "model": self.model,
                "shopFiles": self.shop_files,
                "shopVersion": self.shop_version,
                "templateVersion": self.template_version,
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

        for base_mapping in self.base_mapping:
            edge = self._create_base_mapping_edge(base_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(base_mapping, DomainModelApply):
                instances = base_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for output_definition in self.output_definitions:
            edge = self._create_output_definition_edge(output_definition)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(output_definition, DomainModelApply):
                instances = output_definition._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_base_mapping_edge(self, base_mapping: Union[str, "InputTimeSeriesMappingApply"]) -> dm.EdgeApply:
        if isinstance(base_mapping, str):
            end_node_ext_id = base_mapping
        elif isinstance(base_mapping, DomainModelApply):
            end_node_ext_id = base_mapping.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(base_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ScenarioTemplate.baseMapping"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_output_definition_edge(self, output_definition: Union[str, "OutputMappingApply"]) -> dm.EdgeApply:
        if isinstance(output_definition, str):
            end_node_ext_id = output_definition
        elif isinstance(output_definition, DomainModelApply):
            end_node_ext_id = output_definition.external_id
        else:
            raise TypeError(f"Expected str or OutputMappingApply, got {type(output_definition)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ScenarioTemplate.outputDefinitions"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ScenarioTemplateList(TypeList[ScenarioTemplate]):
    _NODE = ScenarioTemplate
