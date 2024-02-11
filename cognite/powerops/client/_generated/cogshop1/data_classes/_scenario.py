from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._commands_config import CommandsConfigApply
    from ._file_ref import FileRefApply
    from ._mapping import MappingApply
    from ._model_template import ModelTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList", "ScenarioApplyList", "ScenarioFields", "ScenarioTextFields"]


ScenarioTextFields = Literal["name", "source"]
ScenarioFields = Literal["name", "source"]

_SCENARIO_PROPERTIES_BY_FIELD = {"name": "name", "source": "source"}


class Scenario(DomainModel, protected_namespaces=()):
    space: str = "cogShop"
    name: Optional[str] = None
    model_template: Optional[str] = Field(None, alias="modelTemplate")
    commands: Optional[str] = None
    source: Optional[str] = None
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    mappings_override: Optional[list[str]] = Field(None, alias="mappingsOverride")

    def as_apply(self) -> ScenarioApply:
        return ScenarioApply(
            external_id=self.external_id,
            name=self.name,
            model_template=self.model_template,
            commands=self.commands,
            source=self.source,
            extra_files=self.extra_files,
            mappings_override=self.mappings_override,
        )


class ScenarioApply(DomainModelApply, protected_namespaces=()):
    space: str = "cogShop"
    name: str
    model_template: Union[ModelTemplateApply, str, None] = Field(None, repr=False, alias="modelTemplate")
    commands: Union[CommandsConfigApply, str, None] = Field(None, repr=False)
    source: Optional[str] = None
    extra_files: Union[list[FileRefApply], list[str], None] = Field(default=None, repr=False, alias="extraFiles")
    mappings_override: Union[list[MappingApply], list[str], None] = Field(
        default=None, repr=False, alias="mappingsOverride"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.model_template is not None:
            properties["modelTemplate"] = {
                "space": "cogShop",
                "externalId": (
                    self.model_template if isinstance(self.model_template, str) else self.model_template.external_id
                ),
            }
        if self.commands is not None:
            properties["commands"] = {
                "space": "cogShop",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        if self.source is not None:
            properties["source"] = self.source
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

        for extra_file in self.extra_files or []:
            edge = self._create_extra_file_edge(extra_file)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(extra_file, DomainModelApply):
                instances = extra_file._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for mappings_override in self.mappings_override or []:
            edge = self._create_mappings_override_edge(mappings_override)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mappings_override, DomainModelApply):
                instances = mappings_override._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.model_template, DomainModelApply):
            instances = self.model_template._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
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

    def as_apply(self) -> ScenarioApplyList:
        return ScenarioApplyList([node.as_apply() for node in self.data])


class ScenarioApplyList(TypeApplyList[ScenarioApply]):
    _NODE = ScenarioApply
