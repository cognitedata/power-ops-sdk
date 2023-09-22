from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import RKOMProces, RKOMProcesApply, RKOMProcesList


class RKOMProcesIncrementalMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMProcess.incremental_mappings"},
        )
        if isinstance(external_id, str):
            is_rkom_proces = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_proces)
            )

        else:
            is_rkom_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_process)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMProcess.incremental_mappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class RKOMProcessAPI(TypeAPI[RKOMProces, RKOMProcesApply, RKOMProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMProcess", "268dee7a04a5c3"),
            class_type=RKOMProces,
            class_apply_type=RKOMProcesApply,
            class_list=RKOMProcesList,
        )
        self.incremental_mappings = RKOMProcesIncrementalMappingsAPI(client)

    def apply(self, rkom_proces: RKOMProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMProces | RKOMProcesList:
        if isinstance(external_id, str):
            rkom_proces = self._retrieve((self.sources.space, external_id))

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            rkom_proces.incremental_mappings = [edge.end_node.external_id for edge in incremental_mapping_edges]

            return rkom_proces
        else:
            rkom_process = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            self._set_incremental_mappings(rkom_process, incremental_mapping_edges)

            return rkom_process

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> RKOMProcesList:
        rkom_process = self._list(limit=limit)

        incremental_mapping_edges = self.incremental_mappings.list(limit=-1)
        self._set_incremental_mappings(rkom_process, incremental_mapping_edges)

        return rkom_process

    @staticmethod
    def _set_incremental_mappings(rkom_process: Sequence[RKOMProces], incremental_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in incremental_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_proces in rkom_process:
            node_id = rkom_proces.id_tuple()
            if node_id in edges_by_start_node:
                rkom_proces.incremental_mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
