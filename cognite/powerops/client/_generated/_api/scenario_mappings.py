from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import ScenarioMapping, ScenarioMappingApply, ScenarioMappingList


class ScenarioMappingMappingOverridesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioMapping.mappingOverride"},
        )
        if isinstance(external_id, str):
            is_scenario_mapping = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_mapping)
            )

        else:
            is_scenario_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_mappings)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioMapping.mappingOverride"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenarioMappingsAPI(TypeAPI[ScenarioMapping, ScenarioMappingApply, ScenarioMappingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ScenarioMapping", "e65d4465699308"),
            class_type=ScenarioMapping,
            class_apply_type=ScenarioMappingApply,
            class_list=ScenarioMappingList,
        )
        self.mapping_overrides = ScenarioMappingMappingOverridesAPI(client)

    def apply(self, scenario_mapping: ScenarioMappingApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = scenario_mapping.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioMappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ScenarioMappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ScenarioMapping | ScenarioMappingList:
        if isinstance(external_id, str):
            scenario_mapping = self._retrieve((self.sources.space, external_id))

            mapping_override_edges = self.mapping_overrides.retrieve(external_id)
            scenario_mapping.mapping_override = [edge.end_node.external_id for edge in mapping_override_edges]

            return scenario_mapping
        else:
            scenario_mappings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            mapping_override_edges = self.mapping_overrides.retrieve(external_id)
            self._set_mapping_override(scenario_mappings, mapping_override_edges)

            return scenario_mappings

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ScenarioMappingList:
        scenario_mappings = self._list(limit=limit)

        mapping_override_edges = self.mapping_overrides.list(limit=-1)
        self._set_mapping_override(scenario_mappings, mapping_override_edges)

        return scenario_mappings

    @staticmethod
    def _set_mapping_override(scenario_mappings: Sequence[ScenarioMapping], mapping_override_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in mapping_override_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario_mapping in scenario_mappings:
            node_id = scenario_mapping.id_tuple()
            if node_id in edges_by_start_node:
                scenario_mapping.mapping_override = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
