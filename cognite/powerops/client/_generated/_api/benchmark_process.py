from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    BenchmarkProcess,
    BenchmarkProcessApply,
    BenchmarkProcessApplyList,
    BenchmarkProcessList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class BenchmarkProcessProductionPlanTimeSeriesAPI:
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

    def list(self, benchmark_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "BenchmarkProcess.productionPlanTimeSeries"},
        )
        filters.append(is_edge_type)
        if benchmark_proces_id:
            benchmark_proces_ids = (
                [benchmark_proces_id] if isinstance(benchmark_proces_id, str) else benchmark_proces_id
            )
            is_benchmark_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in benchmark_proces_ids],
            )
            filters.append(is_benchmark_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class BenchmarkProcessAPI(TypeAPI[BenchmarkProcess, BenchmarkProcessApply, BenchmarkProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BenchmarkProcess,
            class_apply_type=BenchmarkProcessApply,
            class_list=BenchmarkProcessList,
        )
        self.view_id = view_id
        self.production_plan_time_series = BenchmarkProcessProductionPlanTimeSeriesAPI(client)

    def apply(
        self, benchmark_proces: BenchmarkProcessApply | Sequence[BenchmarkProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(benchmark_proces, BenchmarkProcessApply):
            instances = benchmark_proces.to_instances_apply()
        else:
            instances = BenchmarkProcessApplyList(benchmark_proces).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BenchmarkProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BenchmarkProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BenchmarkProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BenchmarkProcess | BenchmarkProcessList:
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

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BenchmarkProcessList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        benchmark_process = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            production_plan_time_series_edges = self.production_plan_time_series.list(
                benchmark_process.as_external_ids(), limit=-1
            )
            self._set_production_plan_time_series(benchmark_process, production_plan_time_series_edges)

        return benchmark_process

    @staticmethod
    def _set_production_plan_time_series(
        benchmark_process: Sequence[BenchmarkProcess], production_plan_time_series_edges: Sequence[dm.Edge]
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
