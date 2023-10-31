from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    ShopTransformation,
    ShopTransformationApply,
    ShopTransformationApplyList,
    ShopTransformationFields,
    ShopTransformationList,
    ShopTransformationTextFields,
)
from cognite.powerops.client._generated.data_classes._shop_transformation import _SHOPTRANSFORMATION_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class ShopTransformationEndAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ShopTransformation.end"},
        )
        if isinstance(external_id, str):
            is_shop_transformation = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformation)
            )

        else:
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformations)
            )

    def list(
        self, shop_transformation_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ShopTransformation.end"},
        )
        filters.append(is_edge_type)
        if shop_transformation_id:
            shop_transformation_ids = (
                [shop_transformation_id] if isinstance(shop_transformation_id, str) else shop_transformation_id
            )
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in shop_transformation_ids],
            )
            filters.append(is_shop_transformations)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ShopTransformationStartAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ShopTransformation.start"},
        )
        if isinstance(external_id, str):
            is_shop_transformation = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformation)
            )

        else:
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_shop_transformations)
            )

    def list(
        self, shop_transformation_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ShopTransformation.start"},
        )
        filters.append(is_edge_type)
        if shop_transformation_id:
            shop_transformation_ids = (
                [shop_transformation_id] if isinstance(shop_transformation_id, str) else shop_transformation_id
            )
            is_shop_transformations = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in shop_transformation_ids],
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
        self._view_id = view_id
        self.end = ShopTransformationEndAPI(client)
        self.start = ShopTransformationStartAPI(client)

    def apply(
        self, shop_transformation: ShopTransformationApply | Sequence[ShopTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(shop_transformation, ShopTransformationApply):
            instances = shop_transformation.to_instances_apply()
        else:
            instances = ShopTransformationApplyList(shop_transformation).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ShopTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ShopTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ShopTransformation | ShopTransformationList:
        if isinstance(external_id, str):
            shop_transformation = self._retrieve((self._sources.space, external_id))

            end_edges = self.end.retrieve(external_id)
            shop_transformation.end = [edge.end_node.external_id for edge in end_edges]
            start_edges = self.start.retrieve(external_id)
            shop_transformation.start = [edge.end_node.external_id for edge in start_edges]

            return shop_transformation
        else:
            shop_transformations = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            end_edges = self.end.retrieve(external_id)
            self._set_end(shop_transformations, end_edges)
            start_edges = self.start.retrieve(external_id)
            self._set_start(shop_transformations, start_edges)

            return shop_transformations

    def search(
        self,
        query: str,
        properties: ShopTransformationTextFields | Sequence[ShopTransformationTextFields] | None = None,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ShopTransformationList:
        filter_ = _create_filter(
            self._view_id,
            type_name,
            type_name_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _SHOPTRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ShopTransformationFields | Sequence[ShopTransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ShopTransformationTextFields | Sequence[ShopTransformationTextFields] | None = None,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ShopTransformationFields | Sequence[ShopTransformationFields] | None = None,
        group_by: ShopTransformationFields | Sequence[ShopTransformationFields] = None,
        query: str | None = None,
        search_properties: ShopTransformationTextFields | Sequence[ShopTransformationTextFields] | None = None,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ShopTransformationFields | Sequence[ShopTransformationFields] | None = None,
        group_by: ShopTransformationFields | Sequence[ShopTransformationFields] | None = None,
        query: str | None = None,
        search_property: ShopTransformationTextFields | Sequence[ShopTransformationTextFields] | None = None,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            type_name,
            type_name_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPTRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ShopTransformationFields,
        interval: float,
        query: str | None = None,
        search_property: ShopTransformationTextFields | Sequence[ShopTransformationTextFields] | None = None,
        type_name: str | list[str] | None = None,
        type_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            type_name,
            type_name_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPTRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

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
            self._view_id,
            type_name,
            type_name_prefix,
            external_id_prefix,
            filter,
        )

        shop_transformations = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := shop_transformations.as_external_ids()) > IN_FILTER_LIMIT:
                end_edges = self.end.list(limit=-1)
            else:
                end_edges = self.end.list(external_ids, limit=-1)
            self._set_end(shop_transformations, end_edges)
            if len(external_ids := shop_transformations.as_external_ids()) > IN_FILTER_LIMIT:
                start_edges = self.start.list(limit=-1)
            else:
                start_edges = self.start.list(external_ids, limit=-1)
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
