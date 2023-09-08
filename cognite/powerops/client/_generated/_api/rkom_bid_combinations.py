from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    RKOMBidCombination,
    RKOMBidCombinationApply,
    RKOMBidCombinationList,
)


class RKOMBidCombinationsAPI(TypeAPI[RKOMBidCombination, RKOMBidCombinationApply, RKOMBidCombinationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMBidCombination", "b8c2faf6e35abe"),
            class_type=RKOMBidCombination,
            class_apply_type=RKOMBidCombinationApply,
            class_list=RKOMBidCombinationList,
        )

    def apply(self, rkom_bid_combination: RKOMBidCombinationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_bid_combination.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMBidCombinationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMBidCombinationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMBidCombination:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMBidCombinationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMBidCombination | RKOMBidCombinationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> RKOMBidCombinationList:
        return self._list(limit=limit)
