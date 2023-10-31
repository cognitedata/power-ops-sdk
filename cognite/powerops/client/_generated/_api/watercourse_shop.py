from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    WatercourseShop,
    WatercourseShopApply,
    WatercourseShopApplyList,
    WatercourseShopFields,
    WatercourseShopList,
)
from cognite.powerops.client._generated.data_classes._watercourse_shop import _WATERCOURSESHOP_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class WatercourseShopAPI(TypeAPI[WatercourseShop, WatercourseShopApply, WatercourseShopList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WatercourseShop,
            class_apply_type=WatercourseShopApply,
            class_list=WatercourseShopList,
        )
        self._view_id = view_id

    def apply(
        self, watercourse_shop: WatercourseShopApply | Sequence[WatercourseShopApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(watercourse_shop, WatercourseShopApply):
            instances = watercourse_shop.to_instances_apply()
        else:
            instances = WatercourseShopApplyList(watercourse_shop).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WatercourseShop:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WatercourseShopList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WatercourseShop | WatercourseShopList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WatercourseShopFields,
        interval: float,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopList:
        filter_ = _create_filter(
            self._view_id,
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
