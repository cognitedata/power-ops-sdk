from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import Reservoir, ReservoirApply, ReservoirList


class ReservoirsAPI(TypeAPI[Reservoir, ReservoirApply, ReservoirList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Reservoir", "3c822b0c3d68f7"),
            class_type=Reservoir,
            class_apply_type=ReservoirApply,
            class_list=ReservoirList,
        )

    def apply(self, reservoir: ReservoirApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = reservoir.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ReservoirApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ReservoirApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Reservoir:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReservoirList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Reservoir | ReservoirList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ReservoirList:
        return self._list(limit=limit)
