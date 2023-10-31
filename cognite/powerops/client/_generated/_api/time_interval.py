from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    TimeInterval,
    TimeIntervalApply,
    TimeIntervalApplyList,
    TimeIntervalFields,
    TimeIntervalList,
)
from cognite.powerops.client._generated.data_classes._time_interval import _TIMEINTERVAL_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class TimeIntervalAPI(TypeAPI[TimeInterval, TimeIntervalApply, TimeIntervalList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TimeInterval,
            class_apply_type=TimeIntervalApply,
            class_list=TimeIntervalList,
        )
        self._view_id = view_id

    def apply(
        self, time_interval: TimeIntervalApply | Sequence[TimeIntervalApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(time_interval, TimeIntervalApply):
            instances = time_interval.to_instances_apply()
        else:
            instances = TimeIntervalApplyList(time_interval).to_instances_apply()
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
    def retrieve(self, external_id: str) -> TimeInterval:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> TimeIntervalList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> TimeInterval | TimeIntervalList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TimeIntervalFields | Sequence[TimeIntervalFields] | None = None,
        group_by: None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
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
        property: TimeIntervalFields | Sequence[TimeIntervalFields] | None = None,
        group_by: TimeIntervalFields | Sequence[TimeIntervalFields] = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
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
        property: TimeIntervalFields | Sequence[TimeIntervalFields] | None = None,
        group_by: TimeIntervalFields | Sequence[TimeIntervalFields] | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_start,
            max_start,
            min_end,
            max_end,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TIMEINTERVAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TimeIntervalFields,
        interval: float,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_start,
            max_start,
            min_end,
            max_end,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TIMEINTERVAL_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeIntervalList:
        filter_ = _create_filter(
            self._view_id,
            min_start,
            max_start,
            min_end,
            max_end,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_start: datetime.datetime | None = None,
    max_start: datetime.datetime | None = None,
    min_end: datetime.datetime | None = None,
    max_end: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_start or max_start:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("Start"),
                gte=min_start.isoformat() if min_start else None,
                lte=max_start.isoformat() if max_start else None,
            )
        )
    if min_end or max_end:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("End"),
                gte=min_end.isoformat() if min_end else None,
                lte=max_end.isoformat() if max_end else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
