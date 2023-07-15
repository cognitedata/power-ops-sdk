from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import ReserveScenario, ReserveScenarioApply, ReserveScenarioList


class ReserveScenarioOverrideMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ReserveScenario.overrideMappings"},
        )
        if isinstance(external_id, str):
            is_reserve_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_reserve_scenario)
            )

        else:
            is_reserve_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_reserve_scenarios)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ReserveScenario.overrideMappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ReserveScenariosAPI(TypeAPI[ReserveScenario, ReserveScenarioApply, ReserveScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ReserveScenario", "e971c10bd1e893"),
            class_type=ReserveScenario,
            class_apply_type=ReserveScenarioApply,
            class_list=ReserveScenarioList,
        )
        self.override_mappings = ReserveScenarioOverrideMappingsAPI(client)

    def apply(self, reserve_scenario: ReserveScenarioApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = reserve_scenario.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ReserveScenarioApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ReserveScenarioApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ReserveScenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveScenario | ReserveScenarioList:
        if isinstance(external_id, str):
            reserve_scenario = self._retrieve((self.sources.space, external_id))

            override_mapping_edges = self.override_mappings.retrieve(external_id)
            reserve_scenario.override_mappings = [edge.end_node.external_id for edge in override_mapping_edges]

            return reserve_scenario
        else:
            reserve_scenarios = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            override_mapping_edges = self.override_mappings.retrieve(external_id)
            self._set_override_mappings(reserve_scenarios, override_mapping_edges)

            return reserve_scenarios

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ReserveScenarioList:
        reserve_scenarios = self._list(limit=limit)

        override_mapping_edges = self.override_mappings.list(limit=-1)
        self._set_override_mappings(reserve_scenarios, override_mapping_edges)

        return reserve_scenarios

    @staticmethod
    def _set_override_mappings(reserve_scenarios: Sequence[ReserveScenario], override_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in override_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for reserve_scenario in reserve_scenarios:
            node_id = reserve_scenario.id_tuple()
            if node_id in edges_by_start_node:
                reserve_scenario.override_mappings = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
