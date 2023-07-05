from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import CommandsConfig, CommandsConfigApply, CommandsConfigList

from ._core import TypeAPI


class CommandsConfigsAPI(TypeAPI[CommandsConfig, CommandsConfigApply, CommandsConfigList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "CommandsConfig", "697b9d681302c8"),
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

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> CommandsConfigList:
        return self._list(limit=limit)
