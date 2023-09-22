from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import BenchmarkProces, BenchmarkProcesApply, BenchmarkProcesList


class BenchmarkProcesProductionPlanTimeSeriesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkProcess.productionPlanTimeSeries"},
        )
        if isinstance(external_id, str):
            is_benchmark_proces = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_proces)
            )

        else:
            is_benchmark_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_benchmark_process)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkProcess.productionPlanTimeSeries"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class BenchmarkProcessAPI(TypeAPI[BenchmarkProces, BenchmarkProcesApply, BenchmarkProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "BenchmarkProcess", "3c3a0761a5f084"),
            class_type=BenchmarkProces,
            class_apply_type=BenchmarkProcesApply,
            class_list=BenchmarkProcesList,
        )
        self.production_plan_time_series = BenchmarkProcesProductionPlanTimeSeriesAPI(client)

    def apply(self, benchmark_proces: BenchmarkProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = benchmark_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BenchmarkProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BenchmarkProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BenchmarkProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BenchmarkProces | BenchmarkProcesList:
        if isinstance(external_id, str):
            benchmark_proces = self._retrieve((self.sources.space, external_id))

            production_plan_time_series_edges = self.production_plan_time_series.retrieve(external_id)
            benchmark_proces.production_plan_time_series = [
                edge.end_node.external_id for edge in production_plan_time_series_edges
            ]

            return benchmark_proces
        else:
            benchmark_process = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            production_plan_time_series_edges = self.production_plan_time_series.retrieve(external_id)
            self._set_production_plan_time_series(benchmark_process, production_plan_time_series_edges)

            return benchmark_process

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> BenchmarkProcesList:
        benchmark_process = self._list(limit=limit)

        production_plan_time_series_edges = self.production_plan_time_series.list(limit=-1)
        self._set_production_plan_time_series(benchmark_process, production_plan_time_series_edges)

        return benchmark_process

    @staticmethod
    def _set_production_plan_time_series(
        benchmark_process: Sequence[BenchmarkProces], production_plan_time_series_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in production_plan_time_series_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for benchmark_proces in benchmark_process:
            node_id = benchmark_proces.id_tuple()
            if node_id in edges_by_start_node:
                benchmark_proces.production_plan_time_series = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
