from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market_configuration.data_classes import Benchmarking, BenchmarkingApply, BenchmarkingList

from ._core import TypeAPI


class BenchmarkingPlanTimeSeriesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Benchmarking.productionPlanTimeSeries"},
        )
        if isinstance(external_id, str):
            is_benchmarking = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmarking)
            )

        else:
            is_benchmarkings = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmarkings)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Benchmarking.productionPlanTimeSeries"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class BenchmarkingsAPI(TypeAPI[Benchmarking, BenchmarkingApply, BenchmarkingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Benchmarking", "52a1afc9b44b9f"),
            class_type=Benchmarking,
            class_apply_type=BenchmarkingApply,
            class_list=BenchmarkingList,
        )
        self.production_plan_time_series = BenchmarkingPlanTimeSeriesAPI(client)

    def apply(self, benchmarking: BenchmarkingApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = benchmarking.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BenchmarkingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BenchmarkingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Benchmarking:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Benchmarking | BenchmarkingList:
        if isinstance(external_id, str):
            benchmarking = self._retrieve((self.sources.space, external_id))

            production_plan_time_series_edges = self.production_plan_time_series.retrieve(external_id)
            benchmarking.production_plan_time_series = [
                edge.end_node.external_id for edge in production_plan_time_series_edges
            ]

            return benchmarking
        else:
            benchmarkings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            production_plan_time_series_edges = self.production_plan_time_series.retrieve(external_id)
            self._set_production_plan_time_series(benchmarkings, production_plan_time_series_edges)

            return benchmarkings

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BenchmarkingList:
        benchmarkings = self._list(limit=limit)

        production_plan_time_series_edges = self.production_plan_time_series.list(limit=-1)
        self._set_production_plan_time_series(benchmarkings, production_plan_time_series_edges)

        return benchmarkings

    @staticmethod
    def _set_production_plan_time_series(
        benchmarkings: Sequence[Benchmarking], production_plan_time_series_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in production_plan_time_series_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for benchmarking in benchmarkings:
            node_id = benchmarking.id_tuple()
            if node_id in edges_by_start_node:
                benchmarking.production_plan_time_series = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
