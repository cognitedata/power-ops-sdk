from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated.cogshop1._api._core import TypeAPI
from cognite.powerops.client._generated.cogshop1.data_classes import Mapping, MappingApply, MappingList


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Mapping.transformations"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class MappingsAPI(TypeAPI[Mapping, MappingApply, MappingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "Mapping", "623d70ac8b9d1b"),
            class_type=Mapping,
            class_apply_type=MappingApply,
            class_list=MappingList,
        )
        self.transformations = MappingTransformationsAPI(client)

    def apply(self, mapping: MappingApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = mapping.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> MappingList:
        mappings = self._list(limit=limit)

        transformation_edges = self.transformations.list(limit=-1)
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
