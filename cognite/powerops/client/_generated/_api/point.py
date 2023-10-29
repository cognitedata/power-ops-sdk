from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import Point, PointApply, PointApplyList, PointFields, PointList
from cognite.powerops.client._generated.data_classes._point import _POINT_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class PointAPI(TypeAPI[Point, PointApply, PointList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Point,
            class_apply_type=PointApply,
            class_list=PointList,
        )
        self._view_id = view_id

    def apply(self, point: PointApply | Sequence[PointApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(point, PointApply):
            instances = point.to_instances_apply()
        else:
            instances = PointApplyList(point).to_instances_apply()
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
    def retrieve(self, external_id: str) -> Point:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PointList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Point | PointList:
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
        property: PointFields | Sequence[PointFields] | None = None,
        group_by: None = None,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
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
        property: PointFields | Sequence[PointFields] | None = None,
        group_by: PointFields | Sequence[PointFields] = None,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
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
        property: PointFields | Sequence[PointFields] | None = None,
        group_by: PointFields | Sequence[PointFields] | None = None,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_position,
            max_position,
            min_quantity,
            max_quantity,
            min_minimum_quantity,
            max_minimum_quantity,
            min_price_amount,
            max_price_amount,
            min_energy_price_amount,
            max_energy_price_amount,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _POINT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PointFields,
        interval: float,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_position,
            max_position,
            min_quantity,
            max_quantity,
            min_minimum_quantity,
            max_minimum_quantity,
            min_price_amount,
            max_price_amount,
            min_energy_price_amount,
            max_energy_price_amount,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _POINT_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PointList:
        filter_ = _create_filter(
            self._view_id,
            min_position,
            max_position,
            min_quantity,
            max_quantity,
            min_minimum_quantity,
            max_minimum_quantity,
            min_price_amount,
            max_price_amount,
            min_energy_price_amount,
            max_energy_price_amount,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_position: int | None = None,
    max_position: int | None = None,
    min_quantity: float | None = None,
    max_quantity: float | None = None,
    min_minimum_quantity: float | None = None,
    max_minimum_quantity: float | None = None,
    min_price_amount: float | None = None,
    max_price_amount: float | None = None,
    min_energy_price_amount: float | None = None,
    max_energy_price_amount: float | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_position or max_position:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if min_quantity or max_quantity:
        filters.append(dm.filters.Range(view_id.as_property_ref("quantity"), gte=min_quantity, lte=max_quantity))
    if min_minimum_quantity or max_minimum_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("minimumQuantity"), gte=min_minimum_quantity, lte=max_minimum_quantity
            )
        )
    if min_price_amount or max_price_amount:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("priceAmount"), gte=min_price_amount, lte=max_price_amount)
        )
    if min_energy_price_amount or max_energy_price_amount:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("energyPriceAmount"), gte=min_energy_price_amount, lte=max_energy_price_amount
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
