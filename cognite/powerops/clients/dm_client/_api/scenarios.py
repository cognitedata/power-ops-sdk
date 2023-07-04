from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.client.dm_client._api._core import TypeAPI
from cognite.powerops.client.dm_client.data_classes import Scenario, ScenarioApply, ScenarioList


class ScenarioModelTemplateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.modelTemplate"},
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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.modelTemplate"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ScenarioCommandAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.commands"},
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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Scenario.commands"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
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
            sources=dm.ViewId("cogShop", "Scenario", "3f195f52332116"),
            class_type=Scenario,
            class_apply_type=ScenarioApply,
            class_list=ScenarioList,
        )
        self.model_template = ScenarioModelTemplateAPI(client)
        self.command = ScenarioCommandAPI(client)
        self.extra_files = ScenarioExtraFilesAPI(client)
        self.mappings_overrides = ScenarioMappingsOverridesAPI(client)

    def apply(self, scenario: ScenarioApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = scenario.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(nodes=[(ScenarioApply.space, id) for id in external_id])

    @overload
    def retrieve(self, external_id: str) -> Scenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Scenario | ScenarioList:
        if isinstance(external_id, str):
            scenario = self._retrieve(("cogShop", external_id))
            model_template_edges = self.model_template.retrieve(external_id)
            command_edges = self.command.retrieve(external_id)
            extra_file_edges = self.extra_files.retrieve(external_id)
            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            scenario.modelTemplate = model_template_edges[0].end_node.external_id if model_template_edges else None
            scenario.commands = command_edges[0].end_node.external_id if command_edges else None
            scenario.extraFiles = [edge.end_node.external_id for edge in extra_file_edges]
            scenario.mappingsOverride = [edge.end_node.external_id for edge in mappings_override_edges]

            return scenario
        else:
            scenarios = self._retrieve([("cogShop", ext_id) for ext_id in external_id])
            model_template_edges = self.model_template.retrieve(external_id)
            command_edges = self.command.retrieve(external_id)
            extra_file_edges = self.extra_files.retrieve(external_id)
            mappings_override_edges = self.mappings_overrides.retrieve(external_id)
            self._set_model_template(scenarios, model_template_edges)
            self._set_command(scenarios, command_edges)
            self._set_extra_files(scenarios, extra_file_edges)
            self._set_mappings_overrides(scenarios, mappings_override_edges)

            return scenarios

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ScenarioList:
        scenarios = self._list(limit=limit)

        model_template_edges = self.model_template.list(limit=-1)
        command_edges = self.command.list(limit=-1)
        extra_file_edges = self.extra_files.list(limit=-1)
        mappings_override_edges = self.mappings_overrides.list(limit=-1)
        self._set_model_template(scenarios, model_template_edges)
        self._set_command(scenarios, command_edges)
        self._set_extra_files(scenarios, extra_file_edges)
        self._set_mappings_overrides(scenarios, mappings_override_edges)

        return scenarios

    @staticmethod
    def _set_modelTemplate(scenarios: Sequence[Scenario], model_template_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, dm.Edge] = {edge.start_node.as_tuple(): edge for edge in model_template_edges}

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.modelTemplate = edges_by_start_node[node_id].end_node.external_id

    @staticmethod
    def _set_commands(scenarios: Sequence[Scenario], command_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, dm.Edge] = {edge.start_node.as_tuple(): edge for edge in command_edges}

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.commands = edges_by_start_node[node_id].end_node.external_id

    @staticmethod
    def _set_extra_files(scenarios: Sequence[Scenario], extra_file_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in extra_file_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.extraFiles = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_mappings_overrides(scenarios: Sequence[Scenario], mappings_override_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in mappings_override_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for scenario in scenarios:
            node_id = scenario.id_tuple()
            if node_id in edges_by_start_node:
                scenario.mappingsOverride = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
