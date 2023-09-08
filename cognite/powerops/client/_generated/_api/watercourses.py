from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import Watercourse, WatercourseApply, WatercourseList


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Watercourse.plants"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class WatercoursesAPI(TypeAPI[Watercourse, WatercourseApply, WatercourseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Watercourse", "96f5170f35ef70"),
            class_type=Watercourse,
            class_apply_type=WatercourseApply,
            class_list=WatercourseList,
        )
        self.plants = WatercoursePlantsAPI(client)

    def apply(self, watercourse: WatercourseApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = watercourse.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> WatercourseList:
        watercourses = self._list(limit=limit)

        plant_edges = self.plants.list(limit=-1)
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
