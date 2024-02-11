from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.cogshop1.data_classes import (
    CommandsConfig,
    CommandsConfigApply,
    CommandsConfigApplyList,
    CommandsConfigFields,
    CommandsConfigList,
    CommandsConfigTextFields,
)
from cognite.powerops.client._generated.cogshop1.data_classes._commands_config import (
    _COMMANDSCONFIG_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class CommandsConfigAPI(TypeAPI[CommandsConfig, CommandsConfigApply, CommandsConfigList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CommandsConfig,
            class_apply_type=CommandsConfigApply,
            class_list=CommandsConfigList,
        )
        self._view_id = view_id

    def apply(
        self, commands_config: CommandsConfigApply | Sequence[CommandsConfigApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(commands_config, CommandsConfigApply):
            instances = commands_config.to_instances_apply()
        else:
            instances = CommandsConfigApplyList(commands_config).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="cogShop") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CommandsConfig: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CommandsConfigList: ...

    def retrieve(self, external_id: str | Sequence[str]) -> CommandsConfig | CommandsConfigList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: CommandsConfigTextFields | Sequence[CommandsConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandsConfigList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _COMMANDSCONFIG_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CommandsConfigFields | Sequence[CommandsConfigFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CommandsConfigTextFields | Sequence[CommandsConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CommandsConfigFields | Sequence[CommandsConfigFields] | None = None,
        group_by: CommandsConfigFields | Sequence[CommandsConfigFields] = None,
        query: str | None = None,
        search_properties: CommandsConfigTextFields | Sequence[CommandsConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CommandsConfigFields | Sequence[CommandsConfigFields] | None = None,
        group_by: CommandsConfigFields | Sequence[CommandsConfigFields] | None = None,
        query: str | None = None,
        search_property: CommandsConfigTextFields | Sequence[CommandsConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COMMANDSCONFIG_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CommandsConfigFields,
        interval: float,
        query: str | None = None,
        search_property: CommandsConfigTextFields | Sequence[CommandsConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COMMANDSCONFIG_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandsConfigList:
        filter_ = _create_filter(
            self._view_id,
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
