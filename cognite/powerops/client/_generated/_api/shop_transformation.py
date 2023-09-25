from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    ShopTransformation,
    ShopTransformationApply,
    ShopTransformationApplyList,
    ShopTransformationList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ShopTransformationEndAPI:
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

    def list(self, shop_transformation_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.end"},
        )
        filters.append(is_edge_type)
        if shop_transformation_id:
            shop_transformation_ids = (
                [shop_transformation_id] if isinstance(shop_transformation_id, str) else shop_transformation_id
            )
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in shop_transformation_ids],
            )
            filters.append(is_shop_transformations)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ShopTransformationStartAPI:
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

    def list(self, shop_transformation_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ShopTransformation.start"},
        )
        filters.append(is_edge_type)
        if shop_transformation_id:
            shop_transformation_ids = (
                [shop_transformation_id] if isinstance(shop_transformation_id, str) else shop_transformation_id
            )
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in shop_transformation_ids],
            )
            filters.append(is_shop_transformations)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ShopTransformationAPI(TypeAPI[ShopTransformation, ShopTransformationApply, ShopTransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ShopTransformation,
            class_apply_type=ShopTransformationApply,
            class_list=ShopTransformationList,
        )
        self.view_id = view_id
        self.end = ShopTransformationEndAPI(client)
        self.start = ShopTransformationStartAPI(client)

    def apply(
        self, shop_transformation: ShopTransformationApply | Sequence[ShopTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(shop_transformation, ShopTransformationApply):
            instances = shop_transformation.to_instances_apply()
        else:
            instances = ShopTransformationApplyList(shop_transformation).to_instances_apply()
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

            end_edges = self.end.retrieve(external_id)
            shop_transformation.end = [edge.end_node.external_id for edge in end_edges]
            start_edges = self.start.retrieve(external_id)
            shop_transformation.start = [edge.end_node.external_id for edge in start_edges]

            return shop_transformation
        else:
            shop_transformations = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            end_edges = self.end.retrieve(external_id)
            self._set_end(shop_transformations, end_edges)
            start_edges = self.start.retrieve(external_id)
            self._set_start(shop_transformations, start_edges)

            return shop_transformations

    def list(
        self,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ShopTransformationList:
        filter_ = _create_filter(
            self.view_id,
            type_name,
            type_name_prefix,
            external_id_prefix,
            filter,
        )

        shop_transformations = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            end_edges = self.end.list(shop_transformations.as_external_ids(), limit=-1)
            self._set_end(shop_transformations, end_edges)
            start_edges = self.start.list(shop_transformations.as_external_ids(), limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    type_name: str | list[str] | None = None,
    type_name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if type_name and isinstance(type_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("typeName"), value=type_name))
    if type_name and isinstance(type_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("typeName"), values=type_name))
    if type_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("typeName"), value=type_name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
