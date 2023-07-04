from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market_configuration.data_classes import RKOMProces, RKOMProcesApply, RKOMProcesList

from ._core import TypeAPI


class RKOMProcessAPI(TypeAPI[RKOMProces, RKOMProcesApply, RKOMProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMProcess", "86e9666b318187"),
            class_type=RKOMProces,
            class_apply_type=RKOMProcesApply,
            class_list=RKOMProcesList,
        )

    def apply(self, rkom_proces: RKOMProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMProces | RKOMProcesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> RKOMProcesList:
        return self._list(limit=limit)
