from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    Watercourse,
    WatercourseApply,
    WatercourseApplyList,
    WatercourseList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class WatercoursePlantsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Watercourse.plants"},
        )
        if isinstance(external_id, str):
            is_watercourse = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_watercourse)
            )

        else:
            is_watercourses = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_watercourses)
            )

    def list(self, watercourse_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Watercourse.plants"},
        )
        filters.append(is_edge_type)
        if watercourse_id:
            watercourse_ids = [watercourse_id] if isinstance(watercourse_id, str) else watercourse_id
            is_watercourses = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in watercourse_ids],
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
        self.view_id = view_id
        self.plants = WatercoursePlantsAPI(client)

    def apply(
        self, watercourse: WatercourseApply | Sequence[WatercourseApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(watercourse, WatercourseApply):
            instances = watercourse.to_instances_apply()
        else:
            instances = WatercourseApplyList(watercourse).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(WatercourseApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(WatercourseApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Watercourse:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WatercourseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Watercourse | WatercourseList:
        if isinstance(external_id, str):
            watercourse = self._retrieve((self.sources.space, external_id))

            plant_edges = self.plants.retrieve(external_id)
            watercourse.plants = [edge.end_node.external_id for edge in plant_edges]

            return watercourse
        else:
            watercourses = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            plant_edges = self.plants.retrieve(external_id)
            self._set_plants(watercourses, plant_edges)

            return watercourses

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WatercourseList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        watercourses = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            plant_edges = self.plants.list(watercourses.as_external_ids(), limit=-1)
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
