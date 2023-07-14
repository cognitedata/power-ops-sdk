from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import ReserveScenario, ReserveScenarioApply, ReserveScenarioList


class ReserveScenariosAPI(TypeAPI[ReserveScenario, ReserveScenarioApply, ReserveScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ReserveScenario", "e971c10bd1e893"),
            class_type=ReserveScenario,
            class_apply_type=ReserveScenarioApply,
            class_list=ReserveScenarioList,
        )

    def apply(self, reserve_scenario: ReserveScenarioApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = reserve_scenario.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ReserveScenarioApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ReserveScenarioApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ReserveScenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveScenario | ReserveScenarioList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ReserveScenarioList:
        return self._list(limit=limit)
