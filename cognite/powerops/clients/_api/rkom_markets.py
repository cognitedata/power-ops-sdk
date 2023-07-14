from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients._api._core import TypeAPI
from cognite.powerops.clients.data_classes import RKOMMarket, RKOMMarketApply, RKOMMarketList


class RKOMMarketsAPI(TypeAPI[RKOMMarket, RKOMMarketApply, RKOMMarketList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMMarket", "c362cd4abb3d4e"),
            class_type=RKOMMarket,
            class_apply_type=RKOMMarketApply,
            class_list=RKOMMarketList,
        )

    def apply(self, rkom_market: RKOMMarketApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_market.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMMarketApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMMarketApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMMarket:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMMarketList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMMarket | RKOMMarketList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> RKOMMarketList:
        return self._list(limit=limit)
