from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._command_config import CommandConfigApply
    from ._scenario_mapping import ScenarioMappingApply
    from ._scenario_template import ScenarioTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList", "ScenarioApplyList", "ScenarioFields", "ScenarioTextFields"]


ScenarioTextFields = Literal["name"]
ScenarioFields = Literal["name"]

_SCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class Scenario(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    template: Optional[str] = None
    mapping: Optional[str] = None
    commands: Optional[str] = None

    def as_apply(self) -> ScenarioApply:
        return ScenarioApply(
            external_id=self.external_id,
            name=self.name,
            template=self.template,
            mapping=self.mapping,
            commands=self.commands,
        )


class ScenarioApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    template: Union[ScenarioTemplateApply, str, None] = Field(None, repr=False)
    mapping: Union[ScenarioMappingApply, str, None] = Field(None, repr=False)
    commands: Union[CommandConfigApply, str, None] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.template is not None:
            properties["template"] = {
                "space": "power-ops",
                "externalId": self.template if isinstance(self.template, str) else self.template.external_id,
            }
        if self.mapping is not None:
            properties["mapping"] = {
                "space": "power-ops",
                "externalId": self.mapping if isinstance(self.mapping, str) else self.mapping.external_id,
            }
        if self.commands is not None:
            properties["commands"] = {
                "space": "power-ops",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Scenario"),
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

        if isinstance(self.template, DomainModelApply):
            instances = self.template._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.mapping, DomainModelApply):
            instances = self.mapping._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ScenarioList(TypeList[Scenario]):
    _NODE = Scenario

    def as_apply(self) -> ScenarioApplyList:
        return ScenarioApplyList([node.as_apply() for node in self.data])


class ScenarioApplyList(TypeApplyList[ScenarioApply]):
    _NODE = ScenarioApply
