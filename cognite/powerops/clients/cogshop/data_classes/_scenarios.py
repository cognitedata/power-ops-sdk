from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._commands_configs import CommandsConfigApply
    from ._input_time_series_mappings import InputTimeSeriesMappingApply
    from ._scenario_templates import ScenarioTemplateApply

__all__ = ["Scenario", "ScenarioApply", "ScenarioList"]


class Scenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    commands: Optional[str] = None
    mappings_override: list[str] = Field([], alias="mappingsOverride")
    name: Optional[str] = None
    template: Optional[str] = None


class ScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    commands: Optional[Union[str, "CommandsConfigApply"]] = Field(None, repr=False)
    mappings_override: list[Union[str, "InputTimeSeriesMappingApply"]] = Field(default_factory=lambda: [], repr=False)
    name: Optional[str] = None
    template: Optional[Union[str, "ScenarioTemplateApply"]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Scenario"),
            properties={
                "commands": {
                    "space": "power-ops",
                    "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
                },
                "name": self.name,
                "template": {
                    "space": "power-ops",
                    "externalId": self.template if isinstance(self.template, str) else self.template.external_id,
                },
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

        if isinstance(self.template, DomainModelApply):
            instances = self.template._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_mappings_override_edge(
        self, mappings_override: Union[str, "InputTimeSeriesMappingApply"]
    ) -> dm.EdgeApply:
        if isinstance(mappings_override, str):
            end_node_ext_id = mappings_override
        elif isinstance(mappings_override, DomainModelApply):
            end_node_ext_id = mappings_override.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(mappings_override)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Scenario.mappingsOverride"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ScenarioList(TypeList[Scenario]):
    _NODE = Scenario
