from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    RKOMProcess,
    RKOMProcessApply,
    RKOMProcessApplyList,
    RKOMProcessList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class RKOMProcessIncrementalMappingsAPI:
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

    def list(self, rkom_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMProcess.incremental_mappings"},
        )
        filters.append(is_edge_type)
        if rkom_proces_id:
            rkom_proces_ids = [rkom_proces_id] if isinstance(rkom_proces_id, str) else rkom_proces_id
            is_rkom_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in rkom_proces_ids],
            )
            filters.append(is_rkom_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMProcessAPI(TypeAPI[RKOMProcess, RKOMProcessApply, RKOMProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMProcess,
            class_apply_type=RKOMProcessApply,
            class_list=RKOMProcessList,
        )
        self.view_id = view_id
        self.incremental_mappings = RKOMProcessIncrementalMappingsAPI(client)

    def apply(
        self, rkom_proces: RKOMProcessApply | Sequence[RKOMProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(rkom_proces, RKOMProcessApply):
            instances = rkom_proces.to_instances_apply()
        else:
            instances = RKOMProcessApplyList(rkom_proces).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMProcess | RKOMProcessList:
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

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> RKOMProcessList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )

        rkom_process = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            incremental_mapping_edges = self.incremental_mappings.list(rkom_process.as_external_ids(), limit=-1)
            self._set_incremental_mappings(rkom_process, incremental_mapping_edges)

        return rkom_process

    @staticmethod
    def _set_incremental_mappings(rkom_process: Sequence[RKOMProcess], incremental_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in incremental_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_proces in rkom_process:
            node_id = rkom_proces.id_tuple()
            if node_id in edges_by_start_node:
                rkom_proces.incremental_mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
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
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
