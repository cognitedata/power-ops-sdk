from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    Watercourse,
    WatercourseApply,
    WatercourseApplyList,
    WatercourseFields,
    WatercourseList,
    WatercourseTextFields,
)
from cognite.powerops.client._generated.data_classes._watercourse import _WATERCOURSE_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class WatercoursePlantsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Watercourse.plants"},
        )
        if isinstance(external_id, str):
            is_watercourse = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_watercourse)
            )

        else:
            is_watercourses = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_watercourses)
            )

    def list(
        self, watercourse_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Watercourse.plants"},
        )
        filters.append(is_edge_type)
        if watercourse_id:
            watercourse_ids = [watercourse_id] if isinstance(watercourse_id, str) else watercourse_id
            is_watercourses = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in watercourse_ids],
            )
            filters.append(is_watercourses)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WatercourseAPI(TypeAPI[Watercourse, WatercourseApply, WatercourseList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Watercourse,
            class_apply_type=WatercourseApply,
            class_list=WatercourseList,
        )
        self._view_id = view_id
        self.plants = WatercoursePlantsAPI(client)

    def apply(
        self, watercourse: WatercourseApply | Sequence[WatercourseApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(watercourse, WatercourseApply):
            instances = watercourse.to_instances_apply()
        else:
            instances = WatercourseApplyList(watercourse).to_instances_apply()
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
    def retrieve(self, external_id: str) -> Watercourse:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WatercourseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Watercourse | WatercourseList:
        if isinstance(external_id, str):
            watercourse = self._retrieve((self._sources.space, external_id))

            plant_edges = self.plants.retrieve(external_id)
            watercourse.plants = [edge.end_node.external_id for edge in plant_edges]

            return watercourse
        else:
            watercourses = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            plant_edges = self.plants.retrieve(external_id)
            self._set_plants(watercourses, plant_edges)

            return watercourses

    def search(
        self,
        query: str,
        properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            shop,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _WATERCOURSE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: WatercourseFields | Sequence[WatercourseFields] = None,
        query: str | None = None,
        search_properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: WatercourseFields | Sequence[WatercourseFields] | None = None,
        query: str | None = None,
        search_property: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            shop,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WATERCOURSE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WatercourseFields,
        interval: float,
        query: str | None = None,
        search_property: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            shop,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WATERCOURSE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WatercourseList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            shop,
            external_id_prefix,
            filter,
        )

        watercourses = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := watercourses.as_external_ids()) > IN_FILTER_LIMIT:
                plant_edges = self.plants.list(limit=-1)
            else:
                plant_edges = self.plants.list(external_ids, limit=-1)
            self._set_plants(watercourses, plant_edges)

        return watercourses

    @staticmethod
    def _set_plants(watercourses: Sequence[Watercourse], plant_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in plant_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for watercourse in watercourses:
            node_id = watercourse.id_tuple()
            if node_id in edges_by_start_node:
                watercourse.plants = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if shop and isinstance(shop, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shop"), value={"space": "power-ops", "externalId": shop})
        )
    if shop and isinstance(shop, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shop"), value={"space": shop[0], "externalId": shop[1]})
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"), values=[{"space": "power-ops", "externalId": item} for item in shop]
            )
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"), values=[{"space": item[0], "externalId": item[1]} for item in shop]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
