from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import BenchmarkBid, BenchmarkBidApply, BenchmarkBidList


class BenchmarkBidDatesAPI:
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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkBid.date"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class BenchmarkBidsAPI(TypeAPI[BenchmarkBid, BenchmarkBidApply, BenchmarkBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "BenchmarkBid", "cd2ea6a54b92a6"),
            class_type=BenchmarkBid,
            class_apply_type=BenchmarkBidApply,
            class_list=BenchmarkBidList,
        )
        self.dates = BenchmarkBidDatesAPI(client)

    def apply(self, benchmark_bid: BenchmarkBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = benchmark_bid.to_instances_apply()
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

            date_edges = self.dates.retrieve(external_id)
            benchmark_bid.date = [edge.end_node.external_id for edge in date_edges]

            return benchmark_bid
        else:
            benchmark_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.dates.retrieve(external_id)
            self._set_date(benchmark_bids, date_edges)

            return benchmark_bids

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> BenchmarkBidList:
        benchmark_bids = self._list(limit=limit)

        date_edges = self.dates.list(limit=-1)
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
