from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    BenchmarkBid,
    BenchmarkBidApply,
    BenchmarkBidApplyList,
    BenchmarkBidList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class BenchmarkBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkBid.date"},
        )
        if isinstance(external_id, str):
            is_benchmark_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_bid)
            )

        else:
            is_benchmark_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_bids)
            )

    def list(self, benchmark_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkBid.date"},
        )
        filters.append(is_edge_type)
        if benchmark_bid_id:
            benchmark_bid_ids = [benchmark_bid_id] if isinstance(benchmark_bid_id, str) else benchmark_bid_id
            is_benchmark_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in benchmark_bid_ids],
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
        self.view_id = view_id
        self.date = BenchmarkBidDateAPI(client)

    def apply(
        self, benchmark_bid: BenchmarkBidApply | Sequence[BenchmarkBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(benchmark_bid, BenchmarkBidApply):
            instances = benchmark_bid.to_instances_apply()
        else:
            instances = BenchmarkBidApplyList(benchmark_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BenchmarkBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BenchmarkBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BenchmarkBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BenchmarkBid | BenchmarkBidList:
        if isinstance(external_id, str):
            benchmark_bid = self._retrieve((self.sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            benchmark_bid.date = [edge.end_node.external_id for edge in date_edges]

            return benchmark_bid
        else:
            benchmark_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(benchmark_bids, date_edges)

            return benchmark_bids

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BenchmarkBidList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        benchmark_bids = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            date_edges = self.date.list(benchmark_bids.as_external_ids(), limit=-1)
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
