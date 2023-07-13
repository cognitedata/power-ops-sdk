from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market.data_classes import BenchmarkBid, BenchmarkBidApply, BenchmarkBidList

from ._core import TypeAPI


class BenchmarkBidsAPI(TypeAPI[BenchmarkBid, BenchmarkBidApply, BenchmarkBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "BenchmarkBid", "422faaff0660d1"),
            class_type=BenchmarkBid,
            class_apply_type=BenchmarkBidApply,
            class_list=BenchmarkBidList,
        )

    def apply(self, benchmark_bid: BenchmarkBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = benchmark_bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BenchmarkBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BenchmarkBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BenchmarkBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BenchmarkBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BenchmarkBid | BenchmarkBidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BenchmarkBidList:
        return self._list(limit=limit)
