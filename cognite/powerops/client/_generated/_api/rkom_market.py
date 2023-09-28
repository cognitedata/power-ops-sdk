from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    RKOMMarket,
    RKOMMarketApply,
    RKOMMarketApplyList,
    RKOMMarketList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class RKOMMarketAPI(TypeAPI[RKOMMarket, RKOMMarketApply, RKOMMarketList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMMarket,
            class_apply_type=RKOMMarketApply,
            class_list=RKOMMarketList,
        )
        self.view_id = view_id

    def apply(
        self, rkom_market: RKOMMarketApply | Sequence[RKOMMarketApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(rkom_market, RKOMMarketApply):
            instances = rkom_market.to_instances_apply()
        else:
            instances = RKOMMarketApplyList(rkom_market).to_instances_apply()
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

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        min_start_of_week: int | None = None,
        max_start_of_week: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMMarketList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            min_start_of_week,
            max_start_of_week,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    min_start_of_week: int | None = None,
    max_start_of_week: int | None = None,
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
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if min_start_of_week or max_start_of_week:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("startOfWeek"), gte=min_start_of_week, lte=max_start_of_week)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
