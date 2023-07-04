from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

if TYPE_CHECKING:
    from cognite.powerops.client.dm_client.data_classes._commands_configs import CommandsConfigApply
    from cognite.powerops.client.dm_client.data_classes._file_refs import FileRefApply
    from cognite.powerops.client.dm_client.data_classes._mappings import MappingApply
    from cognite.powerops.client.dm_client.data_classes._model_templates import ModelTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList"]


class Scenario(DomainModel):
    space: ClassVar[str] = "cogShop"
    name: Optional[str] = None
    model_template: Optional[str] = Field(None, alias="modelTemplate")
    commands: Optional[str] = None
    extra_files: list[str] = Field([], alias="extraFiles")
    mappings_override: list[str] = Field([], alias="mappingsOverride")


class ScenarioApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    name: str
    model_template: Optional[Union[str, "ModelTemplateApply"]] = None
    commands: Optional[Union[str, "CommandsConfigApply"]] = None
    extra_files: list[Union[str, "FileRefApply"]] = []
    mappings_override: list[Union[str, "MappingApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "Scenario"),
                    properties={
                        "name": self.name,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        if self.model_template is not None:
            edge = self._create_model_template_edge(self.model_template)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.model_template, CircularModelApply):
                instances = self.model_template._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if self.commands is not None:
            edge = self._create_command_edge(self.commands)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.commands, CircularModelApply):
                instances = self.commands._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for extra_file in self.extra_files:
            edge = self._create_extra_file_edge(extra_file)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(extra_file, CircularModelApply):
                instances = extra_file._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for mappings_override in self.mappings_override:
            edge = self._create_mappings_override_edge(mappings_override)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mappings_override, CircularModelApply):
                instances = mappings_override._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_model_template_edge(self, model_template: Union[str, "ModelTemplateApply"]) -> dm.EdgeApply:
        if isinstance(model_template, str):
            end_node_ext_id = model_template
        elif isinstance(model_template, CircularModelApply):
            end_node_ext_id = model_template.external_id
        else:
            raise TypeError(f"Expected str or ModelTemplateApply, got {type(model_template)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.modelTemplate"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_command_edge(self, command: Union[str, "CommandsConfigApply"]) -> dm.EdgeApply:
        if isinstance(command, str):
            end_node_ext_id = command
        elif isinstance(command, CircularModelApply):
            end_node_ext_id = command.external_id
        else:
            raise TypeError(f"Expected str or CommandApply, got {type(command)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.commands"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_extra_file_edge(self, extra_file: Union[str, "FileRefApply"]) -> dm.EdgeApply:
        if isinstance(extra_file, str):
            end_node_ext_id = extra_file
        elif isinstance(extra_file, CircularModelApply):
            end_node_ext_id = extra_file.external_id
        else:
            raise TypeError(f"Expected str or ExtraFileApply, got {type(extra_file)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.extraFiles"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_mappings_override_edge(self, mappings_override: Union[str, "MappingApply"]) -> dm.EdgeApply:
        if isinstance(mappings_override, str):
            end_node_ext_id = mappings_override
        elif isinstance(mappings_override, CircularModelApply):
            end_node_ext_id = mappings_override.external_id
        else:
            raise TypeError(f"Expected str or MappingsOverrideApply, got {type(mappings_override)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.mappingsOverride"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class ScenarioList(TypeList[Scenario]):
    _NODE = Scenario
