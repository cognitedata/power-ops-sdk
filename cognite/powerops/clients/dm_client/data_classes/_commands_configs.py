from __future__ import annotations

from typing import ClassVar

from cognite.client import data_modeling as dm

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

__all__ = ["CommandsConfig", "CommandsConfigApply", "CommandsConfigList"]


class CommandsConfig(DomainModel):
    space: ClassVar[str] = "cogShop"
    commands: list[str] = None


class CommandsConfigApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    commands: list[str]

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "CommandsConfig"),
                    properties={
                        "commands": self.commands,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class CommandsConfigList(TypeList[CommandsConfig]):
    _NODE = CommandsConfig
