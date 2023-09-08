from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import PriceArea, PriceAreaApply, PriceAreaList


class PriceAreaPlantsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.plants"},
        )
        if isinstance(external_id, str):
            is_price_area = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_area)
            )

        else:
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_areas)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.plants"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PriceAreaWatercoursesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.watercourses"},
        )
        if isinstance(external_id, str):
            is_price_area = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_area)
            )

        else:
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_areas)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.watercourses"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PriceAreasAPI(TypeAPI[PriceArea, PriceAreaApply, PriceAreaList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "PriceArea", "6849ae787cd368"),
            class_type=PriceArea,
            class_apply_type=PriceAreaApply,
            class_list=PriceAreaList,
        )
        self.plants = PriceAreaPlantsAPI(client)
        self.watercourses = PriceAreaWatercoursesAPI(client)

    def apply(self, price_area: PriceAreaApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = price_area.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PriceAreaApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PriceAreaApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PriceArea:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PriceAreaList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PriceArea | PriceAreaList:
        if isinstance(external_id, str):
            price_area = self._retrieve((self.sources.space, external_id))

            plant_edges = self.plants.retrieve(external_id)
            price_area.plants = [edge.end_node.external_id for edge in plant_edges]
            watercourse_edges = self.watercourses.retrieve(external_id)
            price_area.watercourses = [edge.end_node.external_id for edge in watercourse_edges]

            return price_area
        else:
            price_areas = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            plant_edges = self.plants.retrieve(external_id)
            self._set_plants(price_areas, plant_edges)
            watercourse_edges = self.watercourses.retrieve(external_id)
            self._set_watercourses(price_areas, watercourse_edges)

            return price_areas

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> PriceAreaList:
        price_areas = self._list(limit=limit)

        plant_edges = self.plants.list(limit=-1)
        self._set_plants(price_areas, plant_edges)
        watercourse_edges = self.watercourses.list(limit=-1)
        self._set_watercourses(price_areas, watercourse_edges)

        return price_areas

    @staticmethod
    def _set_plants(price_areas: Sequence[PriceArea], plant_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in plant_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for price_area in price_areas:
            node_id = price_area.id_tuple()
            if node_id in edges_by_start_node:
                price_area.plants = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_watercourses(price_areas: Sequence[PriceArea], watercourse_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in watercourse_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for price_area in price_areas:
            node_id = price_area.id_tuple()
            if node_id in edges_by_start_node:
                price_area.watercourses = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
