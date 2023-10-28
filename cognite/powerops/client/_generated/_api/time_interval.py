from __future__ import annotations

from collections import defaultdict
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
    TimeIntervalTextFields,
)
from cognite.powerops.client._generated.data_classes._time_interval import _TIMEINTERVAL_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class TimeIntervalParentAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TimeInterval.parent"},
        )
        if isinstance(external_id, str):
            is_time_interval = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_time_interval)
            )

        else:
            is_time_intervals = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_time_intervals)
            )

    def list(
        self, time_interval_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TimeInterval.parent"},
        )
        filters.append(is_edge_type)
        if time_interval_id:
            time_interval_ids = [time_interval_id] if isinstance(time_interval_id, str) else time_interval_id
            is_time_intervals = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in time_interval_ids],
            )
            filters.append(is_time_intervals)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


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
        self.parent = TimeIntervalParentAPI(client)

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
            time_interval = self._retrieve((self._sources.space, external_id))

            parent_edges = self.parent.retrieve(external_id)
            time_interval.parent = [edge.end_node.external_id for edge in parent_edges]

            return time_interval
        else:
            time_intervals = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            parent_edges = self.parent.retrieve(external_id)
            self._set_parent(time_intervals, parent_edges)

            return time_intervals

    def search(
        self,
        query: str,
        properties: TimeIntervalTextFields | Sequence[TimeIntervalTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeIntervalList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _TIMEINTERVAL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TimeIntervalFields | Sequence[TimeIntervalFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: TimeIntervalTextFields | Sequence[TimeIntervalTextFields] | None = None,
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
        query: str | None = None,
        search_properties: TimeIntervalTextFields | Sequence[TimeIntervalTextFields] | None = None,
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
        query: str | None = None,
        search_property: TimeIntervalTextFields | Sequence[TimeIntervalTextFields] | None = None,
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
            _TIMEINTERVAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TimeIntervalFields,
        interval: float,
        query: str | None = None,
        search_property: TimeIntervalTextFields | Sequence[TimeIntervalTextFields] | None = None,
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
            _TIMEINTERVAL_PROPERTIES_BY_FIELD,
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
        retrieve_edges: bool = True,
    ) -> TimeIntervalList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )

        time_intervals = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := time_intervals.as_external_ids()) > IN_FILTER_LIMIT:
                parent_edges = self.parent.list(limit=-1)
            else:
                parent_edges = self.parent.list(external_ids, limit=-1)
            self._set_parent(time_intervals, parent_edges)

        return time_intervals

    @staticmethod
    def _set_parent(time_intervals: Sequence[TimeInterval], parent_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in parent_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for time_interval in time_intervals:
            node_id = time_interval.id_tuple()
            if node_id in edges_by_start_node:
                time_interval.parent = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


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
