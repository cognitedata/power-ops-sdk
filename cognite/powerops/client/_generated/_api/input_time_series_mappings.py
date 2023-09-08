from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
    InputTimeSeriesMappingList,
)


class InputTimeSeriesMappingTransformationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "InputTimeSeriesMapping.transformations"},
        )
        if isinstance(external_id, str):
            is_input_time_series_mapping = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_input_time_series_mapping)
            )

        else:
            is_input_time_series_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_input_time_series_mappings)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "InputTimeSeriesMapping.transformations"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class InputTimeSeriesMappingsAPI(
    TypeAPI[InputTimeSeriesMapping, InputTimeSeriesMappingApply, InputTimeSeriesMappingList]
):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "InputTimeSeriesMapping", "2426123a688e61"),
            class_type=InputTimeSeriesMapping,
            class_apply_type=InputTimeSeriesMappingApply,
            class_list=InputTimeSeriesMappingList,
        )
        self.transformations = InputTimeSeriesMappingTransformationsAPI(client)

    def apply(
        self, input_time_series_mapping: InputTimeSeriesMappingApply, replace: bool = False
    ) -> dm.InstancesApplyResult:
        instances = input_time_series_mapping.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(InputTimeSeriesMappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(InputTimeSeriesMappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> InputTimeSeriesMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> InputTimeSeriesMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> InputTimeSeriesMapping | InputTimeSeriesMappingList:
        if isinstance(external_id, str):
            input_time_series_mapping = self._retrieve((self.sources.space, external_id))

            transformation_edges = self.transformations.retrieve(external_id)
            input_time_series_mapping.transformations = [edge.end_node.external_id for edge in transformation_edges]

            return input_time_series_mapping
        else:
            input_time_series_mappings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            transformation_edges = self.transformations.retrieve(external_id)
            self._set_transformations(input_time_series_mappings, transformation_edges)

            return input_time_series_mappings

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> InputTimeSeriesMappingList:
        input_time_series_mappings = self._list(limit=limit)

        transformation_edges = self.transformations.list(limit=-1)
        self._set_transformations(input_time_series_mappings, transformation_edges)

        return input_time_series_mappings

    @staticmethod
    def _set_transformations(
        input_time_series_mappings: Sequence[InputTimeSeriesMapping], transformation_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in transformation_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for input_time_series_mapping in input_time_series_mappings:
            node_id = input_time_series_mapping.id_tuple()
            if node_id in edges_by_start_node:
                input_time_series_mapping.transformations = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
