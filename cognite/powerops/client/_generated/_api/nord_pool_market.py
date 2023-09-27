from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    NordPoolMarket,
    NordPoolMarketApply,
    NordPoolMarketApplyList,
    NordPoolMarketList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class NordPoolMarketAPI(TypeAPI[NordPoolMarket, NordPoolMarketApply, NordPoolMarketList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=NordPoolMarket,
            class_apply_type=NordPoolMarketApply,
            class_list=NordPoolMarketList,
        )
        self.view_id = view_id

    def apply(
        self, nord_pool_market: NordPoolMarketApply | Sequence[NordPoolMarketApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(nord_pool_market, NordPoolMarketApply):
            instances = nord_pool_market.to_instances_apply()
        else:
            instances = NordPoolMarketApplyList(nord_pool_market).to_instances_apply()
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

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NordPoolMarketList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            min_price_steps,
            max_price_steps,
            price_unit,
            price_unit_prefix,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
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
    min_max_price: float | None = None,
    max_max_price: float | None = None,
    min_min_price: float | None = None,
    max_min_price: float | None = None,
    min_price_steps: int | None = None,
    max_price_steps: int | None = None,
    price_unit: str | list[str] | None = None,
    price_unit_prefix: str | None = None,
    min_tick_size: float | None = None,
    max_tick_size: float | None = None,
    time_unit: str | list[str] | None = None,
    time_unit_prefix: str | None = None,
    min_trade_lot: float | None = None,
    max_trade_lot: float | None = None,
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
    if min_max_price or max_max_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("maxPrice"), gte=min_max_price, lte=max_max_price))
    if min_min_price or max_min_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("minPrice"), gte=min_min_price, lte=max_min_price))
    if min_price_steps or max_price_steps:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("priceSteps"), gte=min_price_steps, lte=max_price_steps)
        )
    if price_unit and isinstance(price_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceUnit"), value=price_unit))
    if price_unit and isinstance(price_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceUnit"), values=price_unit))
    if price_unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceUnit"), value=price_unit_prefix))
    if min_tick_size or max_tick_size:
        filters.append(dm.filters.Range(view_id.as_property_ref("tickSize"), gte=min_tick_size, lte=max_tick_size))
    if time_unit and isinstance(time_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeUnit"), value=time_unit))
    if time_unit and isinstance(time_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeUnit"), values=time_unit))
    if time_unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timeUnit"), value=time_unit_prefix))
    if min_trade_lot or max_trade_lot:
        filters.append(dm.filters.Range(view_id.as_property_ref("tradeLot"), gte=min_trade_lot, lte=max_trade_lot))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
