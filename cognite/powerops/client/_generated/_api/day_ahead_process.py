from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import DayAheadProces, DayAheadProcesApply, DayAheadProcesList


class DayAheadProcesBidMatrixGeneratorConfigsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.bidMatrixGeneratorConfig"},
        )
        if isinstance(external_id, str):
            is_day_ahead_proces = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_proces)
            )

        else:
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_process)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.bidMatrixGeneratorConfig"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class DayAheadProcesIncrementalMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.incremental_mappings"},
        )
        if isinstance(external_id, str):
            is_day_ahead_proces = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_proces)
            )

        else:
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_process)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.incremental_mappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class DayAheadProcessAPI(TypeAPI[DayAheadProces, DayAheadProcesApply, DayAheadProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "DayAheadProcess", "dd1bf62feefc9a"),
            class_type=DayAheadProces,
            class_apply_type=DayAheadProcesApply,
            class_list=DayAheadProcesList,
        )
        self.bid_matrix_generator_configs = DayAheadProcesBidMatrixGeneratorConfigsAPI(client)
        self.incremental_mappings = DayAheadProcesIncrementalMappingsAPI(client)

    def apply(self, day_ahead_proces: DayAheadProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = day_ahead_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadProces | DayAheadProcesList:
        if isinstance(external_id, str):
            day_ahead_proces = self._retrieve((self.sources.space, external_id))

            bid_matrix_generator_config_edges = self.bid_matrix_generator_configs.retrieve(external_id)
            day_ahead_proces.bid_matrix_generator_config = [
                edge.end_node.external_id for edge in bid_matrix_generator_config_edges
            ]
            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            day_ahead_proces.incremental_mappings = [edge.end_node.external_id for edge in incremental_mapping_edges]

            return day_ahead_proces
        else:
            day_ahead_process = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            bid_matrix_generator_config_edges = self.bid_matrix_generator_configs.retrieve(external_id)
            self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)
            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)

            return day_ahead_process

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> DayAheadProcesList:
        day_ahead_process = self._list(limit=limit)

        bid_matrix_generator_config_edges = self.bid_matrix_generator_configs.list(limit=-1)
        self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)
        incremental_mapping_edges = self.incremental_mappings.list(limit=-1)
        self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)

        return day_ahead_process

    @staticmethod
    def _set_bid_matrix_generator_config(
        day_ahead_process: Sequence[DayAheadProces], bid_matrix_generator_config_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in bid_matrix_generator_config_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_proces in day_ahead_process:
            node_id = day_ahead_proces.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_proces.bid_matrix_generator_config = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_incremental_mappings(
        day_ahead_process: Sequence[DayAheadProces], incremental_mapping_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in incremental_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_proces in day_ahead_process:
            node_id = day_ahead_proces.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_proces.incremental_mappings = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
