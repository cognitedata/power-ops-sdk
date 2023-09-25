from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import (
    CommandsConfig,
    CommandsConfigApply,
    CommandsConfigApplyList,
    CommandsConfigList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class CommandsConfigAPI(TypeAPI[CommandsConfig, CommandsConfigApply, CommandsConfigList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CommandsConfig,
            class_apply_type=CommandsConfigApply,
            class_list=CommandsConfigList,
        )
        self.view_id = view_id

    def apply(
        self, commands_config: CommandsConfigApply | Sequence[CommandsConfigApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(commands_config, CommandsConfigApply):
            instances = commands_config.to_instances_apply()
        else:
            instances = CommandsConfigApplyList(commands_config).to_instances_apply()
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

    def list(
        self,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandsConfigList:
        filter_ = _create_filter(
            self.view_id,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
