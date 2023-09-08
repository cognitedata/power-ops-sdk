from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    ShopTransformation,
    ShopTransformationApply,
    ShopTransformationList,
)


class ShopTransformationEndsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.end"},
        )
        if isinstance(external_id, str):
            is_shop_transformation = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformation)
            )

        else:
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformations)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.end"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ShopTransformationStartsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.start"},
        )
        if isinstance(external_id, str):
            is_shop_transformation = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformation)
            )

        else:
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformations)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.start"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ShopTransformationsAPI(TypeAPI[ShopTransformation, ShopTransformationApply, ShopTransformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ShopTransformation", "a74d706d1bda99"),
            class_type=ShopTransformation,
            class_apply_type=ShopTransformationApply,
            class_list=ShopTransformationList,
        )
        self.ends = ShopTransformationEndsAPI(client)
        self.starts = ShopTransformationStartsAPI(client)

    def apply(self, shop_transformation: ShopTransformationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = shop_transformation.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ShopTransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ShopTransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ShopTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ShopTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ShopTransformation | ShopTransformationList:
        if isinstance(external_id, str):
            shop_transformation = self._retrieve((self.sources.space, external_id))

            end_edges = self.ends.retrieve(external_id)
            shop_transformation.end = [edge.end_node.external_id for edge in end_edges]
            start_edges = self.starts.retrieve(external_id)
            shop_transformation.start = [edge.end_node.external_id for edge in start_edges]

            return shop_transformation
        else:
            shop_transformations = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            end_edges = self.ends.retrieve(external_id)
            self._set_end(shop_transformations, end_edges)
            start_edges = self.starts.retrieve(external_id)
            self._set_start(shop_transformations, start_edges)

            return shop_transformations

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ShopTransformationList:
        shop_transformations = self._list(limit=limit)

        end_edges = self.ends.list(limit=-1)
        self._set_end(shop_transformations, end_edges)
        start_edges = self.starts.list(limit=-1)
        self._set_start(shop_transformations, start_edges)

        return shop_transformations

    @staticmethod
    def _set_end(shop_transformations: Sequence[ShopTransformation], end_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in end_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for shop_transformation in shop_transformations:
            node_id = shop_transformation.id_tuple()
            if node_id in edges_by_start_node:
                shop_transformation.end = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_start(shop_transformations: Sequence[ShopTransformation], start_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in start_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for shop_transformation in shop_transformations:
            node_id = shop_transformation.id_tuple()
            if node_id in edges_by_start_node:
                shop_transformation.start = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
