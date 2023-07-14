from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import ScenarioTemplate, ScenarioTemplateApply, ScenarioTemplateList


class ScenarioTemplateBaseMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioTemplate.baseMapping"},
        )
        if isinstance(external_id, str):
            is_scenario_template = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_template)
            )

        else:
            is_scenario_templates = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_templates)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioTemplate.baseMapping"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenarioTemplateOutputDefinitionsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioTemplate.outputDefinitions"},
        )
        if isinstance(external_id, str):
            is_scenario_template = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_template)
            )

        else:
            is_scenario_templates = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_scenario_templates)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioTemplate.outputDefinitions"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenarioTemplatesAPI(TypeAPI[ScenarioTemplate, ScenarioTemplateApply, ScenarioTemplateList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ScenarioTemplate", "77579c65a8cdf9"),
            class_type=ScenarioTemplate,
            class_apply_type=ScenarioTemplateApply,
            class_list=ScenarioTemplateList,
        )
        self.base_mappings = ScenarioTemplateBaseMappingsAPI(client)
        self.output_definitions = ScenarioTemplateOutputDefinitionsAPI(client)

    def apply(self, scenario_template: ScenarioTemplateApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = scenario_template.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioTemplateApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ScenarioTemplateApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioTemplate:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioTemplateList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ScenarioTemplate | ScenarioTemplateList:
        if isinstance(external_id, str):
            scenario_template = self._retrieve((self.sources.space, external_id))

            base_mapping_edges = self.base_mappings.retrieve(external_id)
            scenario_template.base_mapping = [edge.end_node.external_id for edge in base_mapping_edges]
            output_definition_edges = self.output_definitions.retrieve(external_id)
            scenario_template.output_definitions = [edge.end_node.external_id for edge in output_definition_edges]

            return scenario_template
        else:
            scenario_templates = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            base_mapping_edges = self.base_mappings.retrieve(external_id)
            self._set_base_mapping(scenario_templates, base_mapping_edges)
            output_definition_edges = self.output_definitions.retrieve(external_id)
            self._set_output_definitions(scenario_templates, output_definition_edges)

            return scenario_templates

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ScenarioTemplateList:
        scenario_templates = self._list(limit=limit)

        base_mapping_edges = self.base_mappings.list(limit=-1)
        self._set_base_mapping(scenario_templates, base_mapping_edges)
        output_definition_edges = self.output_definitions.list(limit=-1)
        self._set_output_definitions(scenario_templates, output_definition_edges)

        return scenario_templates

    @staticmethod
    def _set_base_mapping(scenario_templates: Sequence[ScenarioTemplate], base_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in base_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario_template in scenario_templates:
            node_id = scenario_template.id_tuple()
            if node_id in edges_by_start_node:
                scenario_template.base_mapping = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_output_definitions(
        scenario_templates: Sequence[ScenarioTemplate], output_definition_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in output_definition_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario_template in scenario_templates:
            node_id = scenario_template.id_tuple()
            if node_id in edges_by_start_node:
                scenario_template.output_definitions = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
