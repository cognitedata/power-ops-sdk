from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import OutputMapping, OutputMappingApply, OutputMappingList


class OutputMappingsAPI(TypeAPI[OutputMapping, OutputMappingApply, OutputMappingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "OutputMapping", "58e6e8f0dadecc"),
            class_type=OutputMapping,
            class_apply_type=OutputMappingApply,
            class_list=OutputMappingList,
        )

    def apply(self, output_mapping: OutputMappingApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = output_mapping.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(OutputMappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(OutputMappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> OutputMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> OutputMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> OutputMapping | OutputMappingList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> OutputMappingList:
        return self._list(limit=limit)
