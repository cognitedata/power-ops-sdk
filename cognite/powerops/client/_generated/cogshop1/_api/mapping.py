from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import (
    Mapping,
    MappingApply,
    MappingApplyList,
    MappingList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class MappingTransformationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Mapping.transformations"},
        )
        if isinstance(external_id, str):
            is_mapping = f.Equals(
                ["edge", "startNode"],
                {"space": "cogShop", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_mapping))

        else:
            is_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_mappings))

    def list(self, mapping_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Mapping.transformations"},
        )
        filters.append(is_edge_type)
        if mapping_id:
            mapping_ids = [mapping_id] if isinstance(mapping_id, str) else mapping_id
            is_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in mapping_ids],
            )
            filters.append(is_mappings)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class MappingAPI(TypeAPI[Mapping, MappingApply, MappingList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Mapping,
            class_apply_type=MappingApply,
            class_list=MappingList,
        )
        self.view_id = view_id
        self.transformations = MappingTransformationsAPI(client)

    def apply(self, mapping: MappingApply | Sequence[MappingApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(mapping, MappingApply):
            instances = mapping.to_instances_apply()
        else:
            instances = MappingApplyList(mapping).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(MappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(MappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Mapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Mapping | MappingList:
        if isinstance(external_id, str):
            mapping = self._retrieve((self.sources.space, external_id))

            transformation_edges = self.transformations.retrieve(external_id)
            mapping.transformations = [edge.end_node.external_id for edge in transformation_edges]

            return mapping
        else:
            mappings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            transformation_edges = self.transformations.retrieve(external_id)
            self._set_transformations(mappings, transformation_edges)

            return mappings

    def list(
        self,
        path: str | list[str] | None = None,
        path_prefix: str | None = None,
        timeseries_external_id: str | list[str] | None = None,
        timeseries_external_id_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> MappingList:
        filter_ = _create_filter(
            self.view_id,
            path,
            path_prefix,
            timeseries_external_id,
            timeseries_external_id_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )

        mappings = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            transformation_edges = self.transformations.list(mappings.as_external_ids(), limit=-1)
            self._set_transformations(mappings, transformation_edges)

        return mappings

    @staticmethod
    def _set_transformations(mappings: Sequence[Mapping], transformation_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in transformation_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for mapping in mappings:
            node_id = mapping.id_tuple()
            if node_id in edges_by_start_node:
                mapping.transformations = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    path: str | list[str] | None = None,
    path_prefix: str | None = None,
    timeseries_external_id: str | list[str] | None = None,
    timeseries_external_id_prefix: str | None = None,
    retrieve: str | list[str] | None = None,
    retrieve_prefix: str | None = None,
    aggregation: str | list[str] | None = None,
    aggregation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if path and isinstance(path, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("path"), value=path))
    if path and isinstance(path, list):
        filters.append(dm.filters.In(view_id.as_property_ref("path"), values=path))
    if path_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("path"), value=path_prefix))
    if timeseries_external_id and isinstance(timeseries_external_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeseriesExternalId"), value=timeseries_external_id))
    if timeseries_external_id and isinstance(timeseries_external_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeseriesExternalId"), values=timeseries_external_id))
    if timeseries_external_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("timeseriesExternalId"), value=timeseries_external_id_prefix)
        )
    if retrieve and isinstance(retrieve, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("retrieve"), value=retrieve))
    if retrieve and isinstance(retrieve, list):
        filters.append(dm.filters.In(view_id.as_property_ref("retrieve"), values=retrieve))
    if retrieve_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("retrieve"), value=retrieve_prefix))
    if aggregation and isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
