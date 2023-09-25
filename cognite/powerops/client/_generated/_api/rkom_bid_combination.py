from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    RKOMBidCombination,
    RKOMBidCombinationApply,
    RKOMBidCombinationApplyList,
    RKOMBidCombinationList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class RKOMBidCombinationAPI(TypeAPI[RKOMBidCombination, RKOMBidCombinationApply, RKOMBidCombinationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMBidCombination,
            class_apply_type=RKOMBidCombinationApply,
            class_list=RKOMBidCombinationList,
        )
        self.view_id = view_id

    def apply(
        self, rkom_bid_combination: RKOMBidCombinationApply | Sequence[RKOMBidCombinationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(rkom_bid_combination, RKOMBidCombinationApply):
            instances = rkom_bid_combination.to_instances_apply()
        else:
            instances = RKOMBidCombinationApplyList(rkom_bid_combination).to_instances_apply()
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

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMBidCombinationList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            auction,
            auction_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    auction: str | list[str] | None = None,
    auction_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if auction and isinstance(auction, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("auction"), value=auction))
    if auction and isinstance(auction, list):
        filters.append(dm.filters.In(view_id.as_property_ref("auction"), values=auction))
    if auction_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("auction"), value=auction_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
