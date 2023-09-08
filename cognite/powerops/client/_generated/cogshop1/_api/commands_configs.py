from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated.cogshop1._api._core import TypeAPI
from cognite.powerops.client._generated.cogshop1.data_classes import (
    CommandsConfig,
    CommandsConfigApply,
    CommandsConfigList,
)


class CommandsConfigsAPI(TypeAPI[CommandsConfig, CommandsConfigApply, CommandsConfigList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "CommandsConfig", "a165239c84ffa9"),
            class_type=CommandsConfig,
            class_apply_type=CommandsConfigApply,
            class_list=CommandsConfigList,
        )

    def apply(self, commands_config: CommandsConfigApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = commands_config.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CommandsConfigApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CommandsConfigApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CommandsConfig:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CommandsConfigList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CommandsConfig | CommandsConfigList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CommandsConfigList:
        return self._list(limit=limit)
