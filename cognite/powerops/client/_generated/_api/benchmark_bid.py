from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    BenchmarkBid,
    BenchmarkBidApply,
    BenchmarkBidApplyList,
    BenchmarkBidFields,
    BenchmarkBidList,
    BenchmarkBidTextFields,
)
from cognite.powerops.client._generated.data_classes._benchmark_bid import _BENCHMARKBID_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class BenchmarkBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BenchmarkBid.date"},
        )
        if isinstance(external_id, str):
            is_benchmark_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_bid)
            )

        else:
            is_benchmark_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_bids)
            )

    def list(
        self, benchmark_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BenchmarkBid.date"},
        )
        filters.append(is_edge_type)
        if benchmark_bid_id:
            benchmark_bid_ids = [benchmark_bid_id] if isinstance(benchmark_bid_id, str) else benchmark_bid_id
            is_benchmark_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in benchmark_bid_ids],
            )
            filters.append(is_benchmark_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class BenchmarkBidAPI(TypeAPI[BenchmarkBid, BenchmarkBidApply, BenchmarkBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BenchmarkBid,
            class_apply_type=BenchmarkBidApply,
            class_list=BenchmarkBidList,
        )
        self._view_id = view_id
        self.date = BenchmarkBidDateAPI(client)

    def apply(
        self, benchmark_bid: BenchmarkBidApply | Sequence[BenchmarkBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(benchmark_bid, BenchmarkBidApply):
            instances = benchmark_bid.to_instances_apply()
        else:
            instances = BenchmarkBidApplyList(benchmark_bid).to_instances_apply()
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
    def retrieve(self, external_id: str) -> BenchmarkBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BenchmarkBid | BenchmarkBidList:
        if isinstance(external_id, str):
            benchmark_bid = self._retrieve((self._sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            benchmark_bid.date = [edge.end_node.external_id for edge in date_edges]

            return benchmark_bid
        else:
            benchmark_bids = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(benchmark_bids, date_edges)

            return benchmark_bids

    def search(
        self,
        query: str,
        properties: BenchmarkBidTextFields | Sequence[BenchmarkBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BenchmarkBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _BENCHMARKBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BenchmarkBidFields | Sequence[BenchmarkBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BenchmarkBidTextFields | Sequence[BenchmarkBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BenchmarkBidFields | Sequence[BenchmarkBidFields] | None = None,
        group_by: BenchmarkBidFields | Sequence[BenchmarkBidFields] = None,
        query: str | None = None,
        search_properties: BenchmarkBidTextFields | Sequence[BenchmarkBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BenchmarkBidFields | Sequence[BenchmarkBidFields] | None = None,
        group_by: BenchmarkBidFields | Sequence[BenchmarkBidFields] | None = None,
        query: str | None = None,
        search_property: BenchmarkBidTextFields | Sequence[BenchmarkBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BENCHMARKBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BenchmarkBidFields,
        interval: float,
        query: str | None = None,
        search_property: BenchmarkBidTextFields | Sequence[BenchmarkBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BENCHMARKBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BenchmarkBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            external_id_prefix,
            filter,
        )

        benchmark_bids = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := benchmark_bids.as_external_ids()) > IN_FILTER_LIMIT:
                date_edges = self.date.list(limit=-1)
            else:
                date_edges = self.date.list(external_ids, limit=-1)
            self._set_date(benchmark_bids, date_edges)

        return benchmark_bids

    @staticmethod
    def _set_date(benchmark_bids: Sequence[BenchmarkBid], date_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in date_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for benchmark_bid in benchmark_bids:
            node_id = benchmark_bid.id_tuple()
            if node_id in edges_by_start_node:
                benchmark_bid.date = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "power-ops", "externalId": market})
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"),
                values=[{"space": "power-ops", "externalId": item} for item in market],
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
