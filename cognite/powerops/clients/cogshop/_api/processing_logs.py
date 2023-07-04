from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import ProcessingLog, ProcessingLogApply, ProcessingLogList

from ._core import TypeAPI


class ProcessingLogsAPI(TypeAPI[ProcessingLog, ProcessingLogApply, ProcessingLogList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "ProcessingLog", "806a9435a2f2df"),
            class_type=ProcessingLog,
            class_apply_type=ProcessingLogApply,
            class_list=ProcessingLogList,
        )

    def apply(self, processing_log: ProcessingLogApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = processing_log.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ProcessingLogApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ProcessingLogApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ProcessingLog:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProcessingLogList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ProcessingLog | ProcessingLogList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ProcessingLogList:
        return self._list(limit=limit)
