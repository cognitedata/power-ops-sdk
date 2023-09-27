from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    WatercourseShop,
    WatercourseShopApply,
    WatercourseShopApplyList,
    WatercourseShopList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class WatercourseShopAPI(TypeAPI[WatercourseShop, WatercourseShopApply, WatercourseShopList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WatercourseShop,
            class_apply_type=WatercourseShopApply,
            class_list=WatercourseShopList,
        )
        self.view_id = view_id

    def apply(
        self, watercourse_shop: WatercourseShopApply | Sequence[WatercourseShopApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(watercourse_shop, WatercourseShopApply):
            instances = watercourse_shop.to_instances_apply()
        else:
            instances = WatercourseShopApplyList(watercourse_shop).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(WatercourseShopApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(WatercourseShopApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WatercourseShop:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WatercourseShopList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WatercourseShop | WatercourseShopList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopList:
        filter_ = _create_filter(
            self.view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_penalty_limit: float | None = None,
    max_penalty_limit: float | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_penalty_limit or max_penalty_limit:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("penaltyLimit"), gte=min_penalty_limit, lte=max_penalty_limit)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
