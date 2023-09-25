from __future__ import annotations

from typing import ClassVar, Optional  # noqa: F401

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["CommandsConfig", "CommandsConfigApply", "CommandsConfigList", "CommandsConfigApplyList"]


class CommandsConfig(DomainModel):
    space: ClassVar[str] = "cogShop"
    commands: list[str] = []

    def as_apply(self) -> CommandsConfigApply:
        return CommandsConfigApply(
            external_id=self.external_id,
            commands=self.commands,
        )


class CommandsConfigApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    commands: list[str]

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.commands is not None:
            properties["commands"] = self.commands
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "CommandsConfig"),
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


class CommandsConfigList(TypeList[CommandsConfig]):
    _NODE = CommandsConfig

    def as_apply(self) -> CommandsConfigApplyList:
        return CommandsConfigApplyList([node.as_apply() for node in self.data])


class CommandsConfigApplyList(TypeApplyList[CommandsConfigApply]):
    _NODE = CommandsConfigApply