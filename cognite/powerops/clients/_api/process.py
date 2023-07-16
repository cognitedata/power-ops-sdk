from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import Proces, ProcesApply, ProcesList


class ProcessAPI(TypeAPI[Proces, ProcesApply, ProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Process", "68e21eeefd0f1c"),
            class_type=Proces,
            class_apply_type=ProcesApply,
            class_list=ProcesList,
        )

    def apply(self, proces: ProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Proces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Proces | ProcesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ProcesList:
        return self._list(limit=limit)
