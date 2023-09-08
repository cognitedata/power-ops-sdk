from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import NordPoolMarket, NordPoolMarketApply, NordPoolMarketList


class NordPoolMarketsAPI(TypeAPI[NordPoolMarket, NordPoolMarketApply, NordPoolMarketList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "NordPoolMarket", "88c86032b9ac9c"),
            class_type=NordPoolMarket,
            class_apply_type=NordPoolMarketApply,
            class_list=NordPoolMarketList,
        )

    def apply(self, nord_pool_market: NordPoolMarketApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = nord_pool_market.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(NordPoolMarketApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(NordPoolMarketApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> NordPoolMarket:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> NordPoolMarketList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> NordPoolMarket | NordPoolMarketList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> NordPoolMarketList:
        return self._list(limit=limit)
