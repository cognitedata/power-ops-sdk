from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    Duration,
    DurationApply,
    DurationApplyList,
    DurationFields,
    DurationList,
    DurationTextFields,
)
from cognite.powerops.client._generated.data_classes._duration import _DURATION_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class DurationAPI(TypeAPI[Duration, DurationApply, DurationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Duration,
            class_apply_type=DurationApply,
            class_list=DurationList,
        )
        self._view_id = view_id

    def apply(
        self, duration: DurationApply | Sequence[DurationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(duration, DurationApply):
            instances = duration.to_instances_apply()
        else:
            instances = DurationApplyList(duration).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Duration:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DurationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Duration | DurationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: DurationTextFields | Sequence[DurationTextFields] | None = None,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DurationList:
        filter_ = _create_filter(
            self._view_id,
            min_duration,
            max_duration,
            unit,
            unit_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _DURATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DurationFields | Sequence[DurationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DurationTextFields | Sequence[DurationTextFields] | None = None,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DurationFields | Sequence[DurationFields] | None = None,
        group_by: DurationFields | Sequence[DurationFields] = None,
        query: str | None = None,
        search_properties: DurationTextFields | Sequence[DurationTextFields] | None = None,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DurationFields | Sequence[DurationFields] | None = None,
        group_by: DurationFields | Sequence[DurationFields] | None = None,
        query: str | None = None,
        search_property: DurationTextFields | Sequence[DurationTextFields] | None = None,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_duration,
            max_duration,
            unit,
            unit_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DURATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DurationFields,
        interval: float,
        query: str | None = None,
        search_property: DurationTextFields | Sequence[DurationTextFields] | None = None,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_duration,
            max_duration,
            unit,
            unit_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DURATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DurationList:
        filter_ = _create_filter(
            self._view_id,
            min_duration,
            max_duration,
            unit,
            unit_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_duration: int | None = None,
    max_duration: int | None = None,
    unit: str | list[str] | None = None,
    unit_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_duration or max_duration:
        filters.append(dm.filters.Range(view_id.as_property_ref("duration"), gte=min_duration, lte=max_duration))
    if unit and isinstance(unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=unit))
    if unit and isinstance(unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unit"), values=unit))
    if unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unit"), value=unit_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
