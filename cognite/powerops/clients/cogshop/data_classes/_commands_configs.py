from __future__ import annotations

from typing import ClassVar

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["CommandsConfig", "CommandsConfigApply", "CommandsConfigList"]


class CommandsConfig(DomainModel):
    space: ClassVar[str] = "cogShop"
    commands: list[str] = []


class CommandsConfigApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    commands: list[str]

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("cogShop", "CommandsConfig"),
            properties={
                "commands": self.commands,
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

        return InstancesApply(nodes, edges)


class CommandsConfigList(TypeList[CommandsConfig]):
    _NODE = CommandsConfig
