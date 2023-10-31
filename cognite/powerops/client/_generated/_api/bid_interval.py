from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    BidInterval,
    BidIntervalApply,
    BidIntervalApplyList,
    BidIntervalFields,
    BidIntervalList,
    BidIntervalTextFields,
)
from cognite.powerops.client._generated.data_classes._bid_interval import _BIDINTERVAL_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class BidIntervalParentAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BidInterval.parent"},
        )
        if isinstance(external_id, str):
            is_bid_interval = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_bid_interval)
            )

        else:
            is_bid_intervals = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_bid_intervals)
            )

    def list(
        self, bid_interval_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BidInterval.parent"},
        )
        filters.append(is_edge_type)
        if bid_interval_id:
            bid_interval_ids = [bid_interval_id] if isinstance(bid_interval_id, str) else bid_interval_id
            is_bid_intervals = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in bid_interval_ids],
            )
            filters.append(is_bid_intervals)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class BidIntervalAPI(TypeAPI[BidInterval, BidIntervalApply, BidIntervalList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidInterval,
            class_apply_type=BidIntervalApply,
            class_list=BidIntervalList,
        )
        self._view_id = view_id
        self.parent = BidIntervalParentAPI(client)

    def apply(
        self, bid_interval: BidIntervalApply | Sequence[BidIntervalApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_interval, BidIntervalApply):
            instances = bid_interval.to_instances_apply()
        else:
            instances = BidIntervalApplyList(bid_interval).to_instances_apply()
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
    def retrieve(self, external_id: str) -> BidInterval:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidIntervalList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidInterval | BidIntervalList:
        if isinstance(external_id, str):
            bid_interval = self._retrieve((self._sources.space, external_id))

            parent_edges = self.parent.retrieve(external_id)
            bid_interval.parent = [edge.end_node.external_id for edge in parent_edges]

            return bid_interval
        else:
            bid_intervals = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            parent_edges = self.parent.retrieve(external_id)
            self._set_parent(bid_intervals, parent_edges)

            return bid_intervals

    def search(
        self,
        query: str,
        properties: BidIntervalTextFields | Sequence[BidIntervalTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidIntervalList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _BIDINTERVAL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidIntervalFields | Sequence[BidIntervalFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidIntervalTextFields | Sequence[BidIntervalTextFields] | None = None,
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
        property: BidIntervalFields | Sequence[BidIntervalFields] | None = None,
        group_by: BidIntervalFields | Sequence[BidIntervalFields] = None,
        query: str | None = None,
        search_properties: BidIntervalTextFields | Sequence[BidIntervalTextFields] | None = None,
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
        property: BidIntervalFields | Sequence[BidIntervalFields] | None = None,
        group_by: BidIntervalFields | Sequence[BidIntervalFields] | None = None,
        query: str | None = None,
        search_property: BidIntervalTextFields | Sequence[BidIntervalTextFields] | None = None,
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
            _BIDINTERVAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidIntervalFields,
        interval: float,
        query: str | None = None,
        search_property: BidIntervalTextFields | Sequence[BidIntervalTextFields] | None = None,
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
            _BIDINTERVAL_PROPERTIES_BY_FIELD,
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
    ) -> BidIntervalList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )

        bid_intervals = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := bid_intervals.as_external_ids()) > IN_FILTER_LIMIT:
                parent_edges = self.parent.list(limit=-1)
            else:
                parent_edges = self.parent.list(external_ids, limit=-1)
            self._set_parent(bid_intervals, parent_edges)

        return bid_intervals

    @staticmethod
    def _set_parent(bid_intervals: Sequence[BidInterval], parent_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in parent_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for bid_interval in bid_intervals:
            node_id = bid_interval.id_tuple()
            if node_id in edges_by_start_node:
                bid_interval.parent = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


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
