from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market_configuration.data_classes import (
    DayAheadProces,
    DayAheadProcesApply,
    DayAheadProcesList,
)

from ._core import TypeAPI


class DayAheadProcessAPI(TypeAPI[DayAheadProces, DayAheadProcesApply, DayAheadProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "DayAheadProcess", "5fb2c51cd3b485"),
            class_type=DayAheadProces,
            class_apply_type=DayAheadProcesApply,
            class_list=DayAheadProcesList,
        )

    def apply(self, day_ahead_proces: DayAheadProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = day_ahead_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadProces | DayAheadProcesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> DayAheadProcesList:
        return self._list(limit=limit)
