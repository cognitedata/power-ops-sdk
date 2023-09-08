from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.cogshop1.data_classes._commands_configs import CommandsConfigApply
    from cognite.powerops.client._generated.cogshop1.data_classes._file_refs import FileRefApply
    from cognite.powerops.client._generated.cogshop1.data_classes._mappings import MappingApply
    from cognite.powerops.client._generated.cogshop1.data_classes._model_templates import ModelTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList"]


class Scenario(DomainModel, protected_namespaces=()):
    space: ClassVar[str] = "cogShop"
    commands: Optional[str] = None
    extra_files: list[str] = Field([], alias="extraFiles")
    mappings_override: list[str] = Field([], alias="mappingsOverride")
    model_template: Optional[str] = Field(None, alias="modelTemplate")
    name: Optional[str] = None


class ScenarioApply(DomainModelApply, protected_namespaces=()):
    space: ClassVar[str] = "cogShop"
    commands: Optional[Union[CommandsConfigApply, str]] = Field(None, repr=False)
    extra_files: list[Union[FileRefApply, str]] = Field(default_factory=list, repr=False)
    mappings_override: list[Union[MappingApply, str]] = Field(default_factory=list, repr=False)
    model_template: Optional[Union[ModelTemplateApply, str]] = Field(None, repr=False)
    name: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.commands is not None:
            properties["commands"] = {
                "space": "cogShop",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        if self.model_template is not None:
            properties["modelTemplate"] = {
                "space": "cogShop",
                "externalId": self.model_template
                if isinstance(self.model_template, str)
                else self.model_template.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(source=dm.ContainerId("cogShop", "Scenario"), properties=properties)
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space, external_id=self.external_id, existing_version=self.existing_version, sources=sources
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for extra_file in self.extra_files:
            edge = self._create_extra_file_edge(extra_file)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(extra_file, DomainModelApply):
                instances = extra_file._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for mappings_override in self.mappings_override:
            edge = self._create_mappings_override_edge(mappings_override)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mappings_override, DomainModelApply):
                instances = mappings_override._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.model_template, DomainModelApply):
            instances = self.model_template._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_extra_file_edge(self, extra_file: Union[str, FileRefApply]) -> dm.EdgeApply:
        if isinstance(extra_file, str):
            end_node_ext_id = extra_file
        elif isinstance(extra_file, DomainModelApply):
            end_node_ext_id = extra_file.external_id
        else:
            raise TypeError(f"Expected str or FileRefApply, got {type(extra_file)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.extraFiles"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_mappings_override_edge(self, mappings_override: Union[str, MappingApply]) -> dm.EdgeApply:
        if isinstance(mappings_override, str):
            end_node_ext_id = mappings_override
        elif isinstance(mappings_override, DomainModelApply):
            end_node_ext_id = mappings_override.external_id
        else:
            raise TypeError(f"Expected str or MappingApply, got {type(mappings_override)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Scenario.mappingsOverride"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class ScenarioList(TypeList[Scenario]):
    _NODE = Scenario
