from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    DayAheadProcess,
    DayAheadProcessApply,
    DayAheadProcessApplyList,
    DayAheadProcessList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class DayAheadProcessIncrementalMappingsAPI:
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

    def list(self, day_ahead_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.incremental_mappings"},
        )
        filters.append(is_edge_type)
        if day_ahead_proces_id:
            day_ahead_proces_ids = (
                [day_ahead_proces_id] if isinstance(day_ahead_proces_id, str) else day_ahead_proces_id
            )
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in day_ahead_proces_ids],
            )
            filters.append(is_day_ahead_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadProcessBidMatrixGeneratorConfigAPI:
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

    def list(self, day_ahead_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadProcess.bidMatrixGeneratorConfig"},
        )
        filters.append(is_edge_type)
        if day_ahead_proces_id:
            day_ahead_proces_ids = (
                [day_ahead_proces_id] if isinstance(day_ahead_proces_id, str) else day_ahead_proces_id
            )
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in day_ahead_proces_ids],
            )
            filters.append(is_day_ahead_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadProcessAPI(TypeAPI[DayAheadProcess, DayAheadProcessApply, DayAheadProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DayAheadProcess,
            class_apply_type=DayAheadProcessApply,
            class_list=DayAheadProcessList,
        )
        self.view_id = view_id
        self.incremental_mappings = DayAheadProcessIncrementalMappingsAPI(client)
        self.bid_matrix_generator_config = DayAheadProcessBidMatrixGeneratorConfigAPI(client)

    def apply(
        self, day_ahead_proces: DayAheadProcessApply | Sequence[DayAheadProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(day_ahead_proces, DayAheadProcessApply):
            instances = day_ahead_proces.to_instances_apply()
        else:
            instances = DayAheadProcessApplyList(day_ahead_proces).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadProcess | DayAheadProcessList:
        if isinstance(external_id, str):
            day_ahead_proces = self._retrieve((self.sources.space, external_id))

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            day_ahead_proces.incremental_mappings = [edge.end_node.external_id for edge in incremental_mapping_edges]
            bid_matrix_generator_config_edges = self.bid_matrix_generator_config.retrieve(external_id)
            day_ahead_proces.bid_matrix_generator_config = [
                edge.end_node.external_id for edge in bid_matrix_generator_config_edges
            ]

            return day_ahead_proces
        else:
            day_ahead_process = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)
            bid_matrix_generator_config_edges = self.bid_matrix_generator_config.retrieve(external_id)
            self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)

            return day_ahead_process

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DayAheadProcessList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        day_ahead_process = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            incremental_mapping_edges = self.incremental_mappings.list(day_ahead_process.as_external_ids(), limit=-1)
            self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)
            bid_matrix_generator_config_edges = self.bid_matrix_generator_config.list(
                day_ahead_process.as_external_ids(), limit=-1
            )
            self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)

        return day_ahead_process

    @staticmethod
    def _set_incremental_mappings(
        day_ahead_process: Sequence[DayAheadProcess], incremental_mapping_edges: Sequence[dm.Edge]
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

    @staticmethod
    def _set_bid_matrix_generator_config(
        day_ahead_process: Sequence[DayAheadProcess], bid_matrix_generator_config_edges: Sequence[dm.Edge]
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
