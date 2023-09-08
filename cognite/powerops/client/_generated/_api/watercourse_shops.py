from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import WatercourseShop, WatercourseShopApply, WatercourseShopList


class WatercourseShopsAPI(TypeAPI[WatercourseShop, WatercourseShopApply, WatercourseShopList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "WatercourseShop", "4b5321b1fccd06"),
            class_type=WatercourseShop,
            class_apply_type=WatercourseShopApply,
            class_list=WatercourseShopList,
        )

    def apply(self, watercourse_shop: WatercourseShopApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = watercourse_shop.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> WatercourseShopList:
        return self._list(limit=limit)
