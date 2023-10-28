from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    BidCurves,
    BidCurvesApply,
    BidCurvesApplyList,
    BidCurvesFields,
    BidCurvesList,
    BidCurvesTextFields,
)
from cognite.powerops.client._generated.data_classes._bid_curves import _BIDCURVES_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class BidCurvesAPI(TypeAPI[BidCurves, BidCurvesApply, BidCurvesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidCurves,
            class_apply_type=BidCurvesApply,
            class_list=BidCurvesList,
        )
        self._view_id = view_id

    def apply(
        self, bid_curve: BidCurvesApply | Sequence[BidCurvesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_curve, BidCurvesApply):
            instances = bid_curve.to_instances_apply()
        else:
            instances = BidCurvesApplyList(bid_curve).to_instances_apply()
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
    def retrieve(self, external_id: str) -> BidCurves:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidCurvesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidCurves | BidCurvesList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidCurvesList:
        filter_ = _create_filter(
            self._view_id,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _BIDCURVES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidCurvesFields | Sequence[BidCurvesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
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
        property: BidCurvesFields | Sequence[BidCurvesFields] | None = None,
        group_by: BidCurvesFields | Sequence[BidCurvesFields] = None,
        query: str | None = None,
        search_properties: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
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
        property: BidCurvesFields | Sequence[BidCurvesFields] | None = None,
        group_by: BidCurvesFields | Sequence[BidCurvesFields] | None = None,
        query: str | None = None,
        search_property: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDCURVES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidCurvesFields,
        interval: float,
        query: str | None = None,
        search_property: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDCURVES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidCurvesList:
        filter_ = _create_filter(
            self._view_id,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    reserve_object: str | list[str] | None = None,
    reserve_object_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if reserve_object and isinstance(reserve_object, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ReserveObject"), value=reserve_object))
    if reserve_object and isinstance(reserve_object, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ReserveObject"), values=reserve_object))
    if reserve_object_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ReserveObject"), value=reserve_object_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
