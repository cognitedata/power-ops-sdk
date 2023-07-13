from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import Scenario, ScenarioApply, ScenarioList

from ._core import TypeAPI


class ScenarioOverridesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Scenario.mappingsOverride"},
        )
        if isinstance(external_id, str):
            is_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenario))

        else:
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenarios))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Scenario.mappingsOverride"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenariosAPI(TypeAPI[Scenario, ScenarioApply, ScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Scenario", "eb6cd945bd1400"),
            class_type=Scenario,
            class_apply_type=ScenarioApply,
            class_list=ScenarioList,
        )
        self.mappings_overrides = ScenarioOverridesAPI(client)

    def apply(self, scenario: ScenarioApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = scenario.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ScenarioApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Scenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Scenario | ScenarioList:
        if isinstance(external_id, str):
            scenario = self._retrieve((self.sources.space, external_id))

            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            scenario.mappings_override = [edge.end_node.external_id for edge in mappings_override_edges]

            return scenario
        else:
            scenarios = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            self._set_mappings_override(scenarios, mappings_override_edges)

            return scenarios

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ScenarioList:
        scenarios = self._list(limit=limit)

        mappings_override_edges = self.mappings_overrides.list(limit=-1)
        self._set_mappings_override(scenarios, mappings_override_edges)

        return scenarios

    @staticmethod
    def _set_mappings_override(scenarios: Sequence[Scenario], mappings_override_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in mappings_override_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.mappings_override = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
