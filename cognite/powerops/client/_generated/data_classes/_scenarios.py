from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._command_configs import CommandConfigApply
    from cognite.powerops.client._generated.data_classes._scenario_mappings import ScenarioMappingApply
    from cognite.powerops.client._generated.data_classes._scenario_templates import ScenarioTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList"]


class Scenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    commands: Optional[str] = None
    mapping: Optional[str] = None
    name: Optional[str] = None
    template: Optional[str] = None


class ScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    commands: Optional[Union[CommandConfigApply, str]] = Field(None, repr=False)
    mapping: Optional[Union[ScenarioMappingApply, str]] = Field(None, repr=False)
    name: Optional[str] = None
    template: Optional[Union[ScenarioTemplateApply, str]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.commands is not None:
            properties["commands"] = {
                "space": "power-ops",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        if self.mapping is not None:
            properties["mapping"] = {
                "space": "power-ops",
                "externalId": self.mapping if isinstance(self.mapping, str) else self.mapping.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if self.template is not None:
            properties["template"] = {
                "space": "power-ops",
                "externalId": self.template if isinstance(self.template, str) else self.template.external_id,
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

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.mapping, DomainModelApply):
            instances = self.mapping._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.template, DomainModelApply):
            instances = self.template._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ScenarioList(TypeList[Scenario]):
    _NODE = Scenario
