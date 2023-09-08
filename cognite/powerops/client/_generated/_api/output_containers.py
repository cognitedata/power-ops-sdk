from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import OutputContainer, OutputContainerApply, OutputContainerList


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "OutputContainer.mappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class OutputContainersAPI(TypeAPI[OutputContainer, OutputContainerApply, OutputContainerList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "OutputContainer", "ad054c0f19ea87"),
            class_type=OutputContainer,
            class_apply_type=OutputContainerApply,
            class_list=OutputContainerList,
        )
        self.mappings = OutputContainerMappingsAPI(client)

    def apply(self, output_container: OutputContainerApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = output_container.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> OutputContainerList:
        output_containers = self._list(limit=limit)

        mapping_edges = self.mappings.list(limit=-1)
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
