from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated.cogshop1._api._core import TypeAPI
from cognite.powerops.client._generated.cogshop1.data_classes import Scenario, ScenarioApply, ScenarioList


class ScenarioExtraFilesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.extraFiles"},
        )
        if isinstance(external_id, str):
            is_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": "cogShop", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenario))

        else:
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenarios))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.extraFiles"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenarioMappingsOverridesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.mappingsOverride"},
        )
        if isinstance(external_id, str):
            is_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": "cogShop", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenario))

        else:
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenarios))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.mappingsOverride"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenariosAPI(TypeAPI[Scenario, ScenarioApply, ScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "Scenario", "7d3086d51c9d6f"),
            class_type=Scenario,
            class_apply_type=ScenarioApply,
            class_list=ScenarioList,
        )
        self.extra_files = ScenarioExtraFilesAPI(client)
        self.mappings_overrides = ScenarioMappingsOverridesAPI(client)

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

            extra_file_edges = self.extra_files.retrieve(external_id)
            scenario.extra_files = [edge.end_node.external_id for edge in extra_file_edges]
            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            scenario.mappings_override = [edge.end_node.external_id for edge in mappings_override_edges]

            return scenario
        else:
            scenarios = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            extra_file_edges = self.extra_files.retrieve(external_id)
            self._set_extra_files(scenarios, extra_file_edges)
            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            self._set_mappings_override(scenarios, mappings_override_edges)

            return scenarios

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ScenarioList:
        scenarios = self._list(limit=limit)

        extra_file_edges = self.extra_files.list(limit=-1)
        self._set_extra_files(scenarios, extra_file_edges)
        mappings_override_edges = self.mappings_overrides.list(limit=-1)
        self._set_mappings_override(scenarios, mappings_override_edges)

        return scenarios

    @staticmethod
    def _set_extra_files(scenarios: Sequence[Scenario], extra_file_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in extra_file_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.extra_files = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_mappings_override(scenarios: Sequence[Scenario], mappings_override_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in mappings_override_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.mappings_override = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
