from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market.data_classes import (
    IncrementalMapping,
    IncrementalMappingApply,
    IncrementalMappingList,
)

from ._core import TypeAPI


class IncrementalMappingMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "IncrementalMapping.mappings"},
        )
        if isinstance(external_id, str):
            is_incremental_mapping = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_incremental_mapping)
            )

        else:
            is_incremental_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_incremental_mappings)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "IncrementalMapping.mappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class IncrementalMappingsAPI(TypeAPI[IncrementalMapping, IncrementalMappingApply, IncrementalMappingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "IncrementalMapping", "eb0f8a7c019c4d"),
            class_type=IncrementalMapping,
            class_apply_type=IncrementalMappingApply,
            class_list=IncrementalMappingList,
        )
        self.mappings = IncrementalMappingMappingsAPI(client)

    def apply(self, incremental_mapping: IncrementalMappingApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = incremental_mapping.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(IncrementalMappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(IncrementalMappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> IncrementalMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> IncrementalMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> IncrementalMapping | IncrementalMappingList:
        if isinstance(external_id, str):
            incremental_mapping = self._retrieve((self.sources.space, external_id))

            mapping_edges = self.mappings.retrieve(external_id)
            incremental_mapping.mappings = [edge.end_node.external_id for edge in mapping_edges]

            return incremental_mapping
        else:
            incremental_mappings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            mapping_edges = self.mappings.retrieve(external_id)
            self._set_mappings(incremental_mappings, mapping_edges)

            return incremental_mappings

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> IncrementalMappingList:
        incremental_mappings = self._list(limit=limit)

        mapping_edges = self.mappings.list(limit=-1)
        self._set_mappings(incremental_mappings, mapping_edges)

        return incremental_mappings

    @staticmethod
    def _set_mappings(incremental_mappings: Sequence[IncrementalMapping], mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for incremental_mapping in incremental_mappings:
            node_id = incremental_mapping.id_tuple()
            if node_id in edges_by_start_node:
                incremental_mapping.mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
