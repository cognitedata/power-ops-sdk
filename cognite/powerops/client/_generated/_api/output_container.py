from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    OutputContainer,
    OutputContainerApply,
    OutputContainerApplyList,
    OutputContainerList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class OutputContainerMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "OutputContainer.mappings"},
        )
        if isinstance(external_id, str):
            is_output_container = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_output_container)
            )

        else:
            is_output_containers = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_output_containers)
            )

    def list(self, output_container_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "OutputContainer.mappings"},
        )
        filters.append(is_edge_type)
        if output_container_id:
            output_container_ids = (
                [output_container_id] if isinstance(output_container_id, str) else output_container_id
            )
            is_output_containers = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in output_container_ids],
            )
            filters.append(is_output_containers)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class OutputContainerAPI(TypeAPI[OutputContainer, OutputContainerApply, OutputContainerList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=OutputContainer,
            class_apply_type=OutputContainerApply,
            class_list=OutputContainerList,
        )
        self.view_id = view_id
        self.mappings = OutputContainerMappingsAPI(client)

    def apply(
        self, output_container: OutputContainerApply | Sequence[OutputContainerApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(output_container, OutputContainerApply):
            instances = output_container.to_instances_apply()
        else:
            instances = OutputContainerApplyList(output_container).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(OutputContainerApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(OutputContainerApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> OutputContainer:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> OutputContainerList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> OutputContainer | OutputContainerList:
        if isinstance(external_id, str):
            output_container = self._retrieve((self.sources.space, external_id))

            mapping_edges = self.mappings.retrieve(external_id)
            output_container.mappings = [edge.end_node.external_id for edge in mapping_edges]

            return output_container
        else:
            output_containers = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            mapping_edges = self.mappings.retrieve(external_id)
            self._set_mappings(output_containers, mapping_edges)

            return output_containers

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
    ) -> OutputContainerList:
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

        output_containers = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            mapping_edges = self.mappings.list(output_containers.as_external_ids(), limit=-1)
            self._set_mappings(output_containers, mapping_edges)

        return output_containers

    @staticmethod
    def _set_mappings(output_containers: Sequence[OutputContainer], mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for output_container in output_containers:
            node_id = output_container.id_tuple()
            if node_id in edges_by_start_node:
                output_container.mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


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
