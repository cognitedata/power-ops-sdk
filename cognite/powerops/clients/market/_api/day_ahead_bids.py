from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market.data_classes import DayAheadBid, DayAheadBidApply, DayAheadBidList

from ._core import TypeAPI


class DayAheadBidsAPI(TypeAPI[DayAheadBid, DayAheadBidApply, DayAheadBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "DayAheadBid", "b791818ef1b125"),
            class_type=DayAheadBid,
            class_apply_type=DayAheadBidApply,
            class_list=DayAheadBidList,
        )

    def apply(self, day_ahead_bid: DayAheadBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = day_ahead_bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadBid | DayAheadBidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> DayAheadBidList:
        return self._list(limit=limit)
