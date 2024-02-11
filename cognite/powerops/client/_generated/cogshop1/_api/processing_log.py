from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.cogshop1.data_classes import (
    ProcessingLog,
    ProcessingLogApply,
    ProcessingLogApplyList,
    ProcessingLogFields,
    ProcessingLogList,
    ProcessingLogTextFields,
)
from cognite.powerops.client._generated.cogshop1.data_classes._processing_log import _PROCESSINGLOG_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class ProcessingLogAPI(TypeAPI[ProcessingLog, ProcessingLogApply, ProcessingLogList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ProcessingLog,
            class_apply_type=ProcessingLogApply,
            class_list=ProcessingLogList,
        )
        self._view_id = view_id

    def apply(
        self, processing_log: ProcessingLogApply | Sequence[ProcessingLogApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(processing_log, ProcessingLogApply):
            instances = processing_log.to_instances_apply()
        else:
            instances = ProcessingLogApplyList(processing_log).to_instances_apply()
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
    def retrieve(self, external_id: str) -> ProcessingLog: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProcessingLogList: ...

    def retrieve(self, external_id: str | Sequence[str]) -> ProcessingLog | ProcessingLogList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ProcessingLogTextFields | Sequence[ProcessingLogTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessingLogList:
        filter_ = _create_filter(
            self._view_id,
            state,
            state_prefix,
            timestamp,
            timestamp_prefix,
            error_message,
            error_message_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _PROCESSINGLOG_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ProcessingLogFields | Sequence[ProcessingLogFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ProcessingLogTextFields | Sequence[ProcessingLogTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
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
        property: ProcessingLogFields | Sequence[ProcessingLogFields] | None = None,
        group_by: ProcessingLogFields | Sequence[ProcessingLogFields] = None,
        query: str | None = None,
        search_properties: ProcessingLogTextFields | Sequence[ProcessingLogTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
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
        property: ProcessingLogFields | Sequence[ProcessingLogFields] | None = None,
        group_by: ProcessingLogFields | Sequence[ProcessingLogFields] | None = None,
        query: str | None = None,
        search_property: ProcessingLogTextFields | Sequence[ProcessingLogTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            state,
            state_prefix,
            timestamp,
            timestamp_prefix,
            error_message,
            error_message_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PROCESSINGLOG_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ProcessingLogFields,
        interval: float,
        query: str | None = None,
        search_property: ProcessingLogTextFields | Sequence[ProcessingLogTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            state,
            state_prefix,
            timestamp,
            timestamp_prefix,
            error_message,
            error_message_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PROCESSINGLOG_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        timestamp: str | list[str] | None = None,
        timestamp_prefix: str | None = None,
        error_message: str | list[str] | None = None,
        error_message_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessingLogList:
        filter_ = _create_filter(
            self._view_id,
            state,
            state_prefix,
            timestamp,
            timestamp_prefix,
            error_message,
            error_message_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    timestamp: str | list[str] | None = None,
    timestamp_prefix: str | None = None,
    error_message: str | list[str] | None = None,
    error_message_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if state and isinstance(state, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("state"), value=state))
    if state and isinstance(state, list):
        filters.append(dm.filters.In(view_id.as_property_ref("state"), values=state))
    if state_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("state"), value=state_prefix))
    if timestamp and isinstance(timestamp, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timestamp"), value=timestamp))
    if timestamp and isinstance(timestamp, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timestamp"), values=timestamp))
    if timestamp_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timestamp"), value=timestamp_prefix))
    if error_message and isinstance(error_message, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("errorMessage"), value=error_message))
    if error_message and isinstance(error_message, list):
        filters.append(dm.filters.In(view_id.as_property_ref("errorMessage"), values=error_message))
    if error_message_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("errorMessage"), value=error_message_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
