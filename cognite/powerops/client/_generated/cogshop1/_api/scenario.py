from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.cogshop1.data_classes import (
    Scenario,
    ScenarioApply,
    ScenarioApplyList,
    ScenarioFields,
    ScenarioList,
    ScenarioTextFields,
)
from cognite.powerops.client._generated.cogshop1.data_classes._scenario import _SCENARIO_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class ScenarioExtraFilesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="cogShop") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Scenario.extraFiles"},
        )
        if isinstance(external_id, str):
            is_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenario))

        else:
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenarios))

    def list(
        self, scenario_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="cogShop"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Scenario.extraFiles"},
        )
        filters.append(is_edge_type)
        if scenario_id:
            scenario_ids = [scenario_id] if isinstance(scenario_id, str) else scenario_id
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in scenario_ids],
            )
            filters.append(is_scenarios)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ScenarioMappingsOverrideAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="cogShop") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Scenario.mappingsOverride"},
        )
        if isinstance(external_id, str):
            is_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenario))

        else:
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_scenarios))

    def list(
        self, scenario_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="cogShop"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Scenario.mappingsOverride"},
        )
        filters.append(is_edge_type)
        if scenario_id:
            scenario_ids = [scenario_id] if isinstance(scenario_id, str) else scenario_id
            is_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in scenario_ids],
            )
            filters.append(is_scenarios)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ScenarioAPI(TypeAPI[Scenario, ScenarioApply, ScenarioList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Scenario,
            class_apply_type=ScenarioApply,
            class_list=ScenarioList,
        )
        self._view_id = view_id
        self.extra_files = ScenarioExtraFilesAPI(client)
        self.mappings_override = ScenarioMappingsOverrideAPI(client)

    def apply(
        self, scenario: ScenarioApply | Sequence[ScenarioApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(scenario, ScenarioApply):
            instances = scenario.to_instances_apply()
        else:
            instances = ScenarioApplyList(scenario).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="cogShop") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Scenario: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioList: ...

    def retrieve(self, external_id: str | Sequence[str]) -> Scenario | ScenarioList:
        if isinstance(external_id, str):
            scenario = self._retrieve((self._sources.space, external_id))

            extra_file_edges = self.extra_files.retrieve(external_id)
            scenario.extra_files = [edge.end_node.external_id for edge in extra_file_edges]
            mappings_override_edges = self.mappings_override.retrieve(external_id)
            scenario.mappings_override = [edge.end_node.external_id for edge in mappings_override_edges]

            return scenario
        else:
            scenarios = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            extra_file_edges = self.extra_files.retrieve(external_id)
            self._set_extra_files(scenarios, extra_file_edges)
            mappings_override_edges = self.mappings_override.retrieve(external_id)
            self._set_mappings_override(scenarios, mappings_override_edges)

            return scenarios

    def search(
        self,
        query: str,
        properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: ScenarioFields | Sequence[ScenarioFields] = None,
        query: str | None = None,
        search_properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: ScenarioFields | Sequence[ScenarioFields] | None = None,
        query: str | None = None,
        search_property: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIO_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ScenarioList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            filter,
        )

        scenarios = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := scenarios.as_external_ids()) > IN_FILTER_LIMIT:
                extra_file_edges = self.extra_files.list(limit=-1)
            else:
                extra_file_edges = self.extra_files.list(external_ids, limit=-1)
            self._set_extra_files(scenarios, extra_file_edges)
            if len(external_ids := scenarios.as_external_ids()) > IN_FILTER_LIMIT:
                mappings_override_edges = self.mappings_override.list(limit=-1)
            else:
                mappings_override_edges = self.mappings_override.list(external_ids, limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
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
    if model_template and isinstance(model_template, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"), value={"space": "cogShop", "externalId": model_template}
            )
        )
    if model_template and isinstance(model_template, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"),
                value={"space": model_template[0], "externalId": model_template[1]},
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": "cogShop", "externalId": item} for item in model_template],
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": item[0], "externalId": item[1]} for item in model_template],
            )
        )
    if commands and isinstance(commands, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("commands"), value={"space": "cogShop", "externalId": commands})
        )
    if commands and isinstance(commands, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"), value={"space": commands[0], "externalId": commands[1]}
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": "cogShop", "externalId": item} for item in commands],
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": item[0], "externalId": item[1]} for item in commands],
            )
        )
    if source and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
