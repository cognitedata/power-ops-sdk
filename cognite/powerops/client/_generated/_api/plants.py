from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import Plant, PlantApply, PlantList


class PlantGeneratorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Plant.generators"},
        )
        if isinstance(external_id, str):
            is_plant = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_plant))

        else:
            is_plants = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_plants))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Plant.generators"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PlantInletReservoirsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Plant.inletReservoirs"},
        )
        if isinstance(external_id, str):
            is_plant = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_plant))

        else:
            is_plants = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_plants))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Plant.inletReservoirs"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PlantsAPI(TypeAPI[Plant, PlantApply, PlantList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Plant", "836dcb3f5da1df"),
            class_type=Plant,
            class_apply_type=PlantApply,
            class_list=PlantList,
        )
        self.generators = PlantGeneratorsAPI(client)
        self.inlet_reservoirs = PlantInletReservoirsAPI(client)

    def apply(self, plant: PlantApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = plant.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PlantApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PlantApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Plant:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PlantList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Plant | PlantList:
        if isinstance(external_id, str):
            plant = self._retrieve((self.sources.space, external_id))

            generator_edges = self.generators.retrieve(external_id)
            plant.generators = [edge.end_node.external_id for edge in generator_edges]
            inlet_reservoir_edges = self.inlet_reservoirs.retrieve(external_id)
            plant.inlet_reservoirs = [edge.end_node.external_id for edge in inlet_reservoir_edges]

            return plant
        else:
            plants = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            generator_edges = self.generators.retrieve(external_id)
            self._set_generators(plants, generator_edges)
            inlet_reservoir_edges = self.inlet_reservoirs.retrieve(external_id)
            self._set_inlet_reservoirs(plants, inlet_reservoir_edges)

            return plants

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> PlantList:
        plants = self._list(limit=limit)

        generator_edges = self.generators.list(limit=-1)
        self._set_generators(plants, generator_edges)
        inlet_reservoir_edges = self.inlet_reservoirs.list(limit=-1)
        self._set_inlet_reservoirs(plants, inlet_reservoir_edges)

        return plants

    @staticmethod
    def _set_generators(plants: Sequence[Plant], generator_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in generator_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for plant in plants:
            node_id = plant.id_tuple()
            if node_id in edges_by_start_node:
                plant.generators = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_inlet_reservoirs(plants: Sequence[Plant], inlet_reservoir_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in inlet_reservoir_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for plant in plants:
            node_id = plant.id_tuple()
            if node_id in edges_by_start_node:
                plant.inlet_reservoirs = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
