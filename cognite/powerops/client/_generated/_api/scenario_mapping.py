from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    ScenarioMapping,
    ScenarioMappingApply,
    ScenarioMappingApplyList,
    ScenarioMappingList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ScenarioMappingMappingOverrideAPI:
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

    def list(self, scenario_mapping_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "ScenarioMapping.mappingOverride"},
        )
        filters.append(is_edge_type)
        if scenario_mapping_id:
            scenario_mapping_ids = (
                [scenario_mapping_id] if isinstance(scenario_mapping_id, str) else scenario_mapping_id
            )
            is_scenario_mappings = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in scenario_mapping_ids],
            )
            filters.append(is_scenario_mappings)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ScenarioMappingAPI(TypeAPI[ScenarioMapping, ScenarioMappingApply, ScenarioMappingList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioMapping,
            class_apply_type=ScenarioMappingApply,
            class_list=ScenarioMappingList,
        )
        self.view_id = view_id
        self.mapping_override = ScenarioMappingMappingOverrideAPI(client)

    def apply(
        self, scenario_mapping: ScenarioMappingApply | Sequence[ScenarioMappingApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(scenario_mapping, ScenarioMappingApply):
            instances = scenario_mapping.to_instances_apply()
        else:
            instances = ScenarioMappingApplyList(scenario_mapping).to_instances_apply()
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

            mapping_override_edges = self.mapping_override.retrieve(external_id)
            scenario_mapping.mapping_override = [edge.end_node.external_id for edge in mapping_override_edges]

            return scenario_mapping
        else:
            scenario_mappings = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            mapping_override_edges = self.mapping_override.retrieve(external_id)
            self._set_mapping_override(scenario_mappings, mapping_override_edges)

            return scenario_mappings

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_type: str | list[str] | None = None,
        shop_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ScenarioMappingList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            watercourse,
            watercourse_prefix,
            shop_type,
            shop_type_prefix,
            external_id_prefix,
            filter,
        )

        scenario_mappings = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            mapping_override_edges = self.mapping_override.list(scenario_mappings.as_external_ids(), limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    watercourse: str | list[str] | None = None,
    watercourse_prefix: str | None = None,
    shop_type: str | list[str] | None = None,
    shop_type_prefix: str | None = None,
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
    if watercourse and isinstance(watercourse, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("watercourse"), value=watercourse))
    if watercourse and isinstance(watercourse, list):
        filters.append(dm.filters.In(view_id.as_property_ref("watercourse"), values=watercourse))
    if watercourse_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("watercourse"), value=watercourse_prefix))
    if shop_type and isinstance(shop_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopType"), value=shop_type))
    if shop_type and isinstance(shop_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopType"), values=shop_type))
    if shop_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopType"), value=shop_type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
