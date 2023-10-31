from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "CommandConfig",
    "CommandConfigApply",
    "CommandConfigList",
    "CommandConfigApplyList",
    "CommandConfigFields",
    "CommandConfigTextFields",
]


CommandConfigTextFields = Literal["commands"]
CommandConfigFields = Literal["commands"]

_COMMANDCONFIG_PROPERTIES_BY_FIELD = {
    "commands": "commands",
}


class CommandConfig(DomainModel):
    space: str = "power-ops"
    commands: Optional[list[str]] = None

    def as_apply(self) -> CommandConfigApply:
        return CommandConfigApply(
            external_id=self.external_id,
            commands=self.commands,
        )


class CommandConfigApply(DomainModelApply):
    space: str = "power-ops"
    commands: Optional[list[str]] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.commands is not None:
            properties["commands"] = self.commands
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "CommandConfig"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CommandConfigList(TypeList[CommandConfig]):
    _NODE = CommandConfig

    def as_apply(self) -> CommandConfigApplyList:
        return CommandConfigApplyList([node.as_apply() for node in self.data])


class CommandConfigApplyList(TypeApplyList[CommandConfigApply]):
    _NODE = CommandConfigApply
