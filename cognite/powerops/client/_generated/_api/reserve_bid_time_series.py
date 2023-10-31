from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    ReserveBidTimeSeries,
    ReserveBidTimeSeriesApply,
    ReserveBidTimeSeriesApplyList,
    ReserveBidTimeSeriesFields,
    ReserveBidTimeSeriesList,
    ReserveBidTimeSeriesTextFields,
)
from cognite.powerops.client._generated.data_classes._reserve_bid_time_series import (
    _RESERVEBIDTIMESERIES_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class ReserveBidTimeSeriesPeriodsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ReserveBidTimeSeries.Periods"},
        )
        if isinstance(external_id, str):
            is_reserve_bid_time_series = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_reserve_bid_time_series)
            )

        else:
            is_reserve_bid_time_series_list = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_reserve_bid_time_series_list)
            )

    def list(
        self, reserve_bid_time_series_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ReserveBidTimeSeries.Periods"},
        )
        filters.append(is_edge_type)
        if reserve_bid_time_series_id:
            reserve_bid_time_series_ids = (
                [reserve_bid_time_series_id]
                if isinstance(reserve_bid_time_series_id, str)
                else reserve_bid_time_series_id
            )
            is_reserve_bid_time_series_list = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in reserve_bid_time_series_ids],
            )
            filters.append(is_reserve_bid_time_series_list)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ReserveBidTimeSeriesAPI(TypeAPI[ReserveBidTimeSeries, ReserveBidTimeSeriesApply, ReserveBidTimeSeriesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ReserveBidTimeSeries,
            class_apply_type=ReserveBidTimeSeriesApply,
            class_list=ReserveBidTimeSeriesList,
        )
        self._view_id = view_id
        self.periods = ReserveBidTimeSeriesPeriodsAPI(client)

    def apply(
        self,
        reserve_bid_time_series: ReserveBidTimeSeriesApply | Sequence[ReserveBidTimeSeriesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(reserve_bid_time_series, ReserveBidTimeSeriesApply):
            instances = reserve_bid_time_series.to_instances_apply()
        else:
            instances = ReserveBidTimeSeriesApplyList(reserve_bid_time_series).to_instances_apply()
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
    def retrieve(self, external_id: str) -> ReserveBidTimeSeries:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveBidTimeSeriesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveBidTimeSeries | ReserveBidTimeSeriesList:
        if isinstance(external_id, str):
            reserve_bid_time_series = self._retrieve((self._sources.space, external_id))

            period_edges = self.periods.retrieve(external_id)
            reserve_bid_time_series.periods = [edge.end_node.external_id for edge in period_edges]

            return reserve_bid_time_series
        else:
            reserve_bid_time_series_list = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            period_edges = self.periods.retrieve(external_id)
            self._set_periods(reserve_bid_time_series_list, period_edges)

            return reserve_bid_time_series_list

    def search(
        self,
        query: str,
        properties: ReserveBidTimeSeriesTextFields | Sequence[ReserveBidTimeSeriesTextFields] | None = None,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveBidTimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            bid_type,
            bid_type_prefix,
            measure_unit,
            measure_unit_prefix,
            currency,
            currency_prefix,
            min_price,
            max_price,
            direction_name,
            direction_name_prefix,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RESERVEBIDTIMESERIES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ReserveBidTimeSeriesFields | Sequence[ReserveBidTimeSeriesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ReserveBidTimeSeriesTextFields | Sequence[ReserveBidTimeSeriesTextFields] | None = None,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
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
        property: ReserveBidTimeSeriesFields | Sequence[ReserveBidTimeSeriesFields] | None = None,
        group_by: ReserveBidTimeSeriesFields | Sequence[ReserveBidTimeSeriesFields] = None,
        query: str | None = None,
        search_properties: ReserveBidTimeSeriesTextFields | Sequence[ReserveBidTimeSeriesTextFields] | None = None,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
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
        property: ReserveBidTimeSeriesFields | Sequence[ReserveBidTimeSeriesFields] | None = None,
        group_by: ReserveBidTimeSeriesFields | Sequence[ReserveBidTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: ReserveBidTimeSeriesTextFields | Sequence[ReserveBidTimeSeriesTextFields] | None = None,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            bid_type,
            bid_type_prefix,
            measure_unit,
            measure_unit_prefix,
            currency,
            currency_prefix,
            min_price,
            max_price,
            direction_name,
            direction_name_prefix,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RESERVEBIDTIMESERIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ReserveBidTimeSeriesFields,
        interval: float,
        query: str | None = None,
        search_property: ReserveBidTimeSeriesTextFields | Sequence[ReserveBidTimeSeriesTextFields] | None = None,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            bid_type,
            bid_type_prefix,
            measure_unit,
            measure_unit_prefix,
            currency,
            currency_prefix,
            min_price,
            max_price,
            direction_name,
            direction_name_prefix,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RESERVEBIDTIMESERIES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        bid_type: str | list[str] | None = None,
        bid_type_prefix: str | None = None,
        measure_unit: str | list[str] | None = None,
        measure_unit_prefix: str | None = None,
        currency: str | list[str] | None = None,
        currency_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        direction_name: str | list[str] | None = None,
        direction_name_prefix: str | None = None,
        reserve_object: str | list[str] | None = None,
        reserve_object_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ReserveBidTimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            bid_type,
            bid_type_prefix,
            measure_unit,
            measure_unit_prefix,
            currency,
            currency_prefix,
            min_price,
            max_price,
            direction_name,
            direction_name_prefix,
            reserve_object,
            reserve_object_prefix,
            external_id_prefix,
            filter,
        )

        reserve_bid_time_series_list = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := reserve_bid_time_series_list.as_external_ids()) > IN_FILTER_LIMIT:
                period_edges = self.periods.list(limit=-1)
            else:
                period_edges = self.periods.list(external_ids, limit=-1)
            self._set_periods(reserve_bid_time_series_list, period_edges)

        return reserve_bid_time_series_list

    @staticmethod
    def _set_periods(reserve_bid_time_series_list: Sequence[ReserveBidTimeSeries], period_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in period_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for reserve_bid_time_series in reserve_bid_time_series_list:
            node_id = reserve_bid_time_series.id_tuple()
            if node_id in edges_by_start_node:
                reserve_bid_time_series.periods = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    bid_type: str | list[str] | None = None,
    bid_type_prefix: str | None = None,
    measure_unit: str | list[str] | None = None,
    measure_unit_prefix: str | None = None,
    currency: str | list[str] | None = None,
    currency_prefix: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    direction_name: str | list[str] | None = None,
    direction_name_prefix: str | None = None,
    reserve_object: str | list[str] | None = None,
    reserve_object_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if bid_type and isinstance(bid_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BidType"), value=bid_type))
    if bid_type and isinstance(bid_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BidType"), values=bid_type))
    if bid_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("BidType"), value=bid_type_prefix))
    if measure_unit and isinstance(measure_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("MeasureUnit"), value=measure_unit))
    if measure_unit and isinstance(measure_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("MeasureUnit"), values=measure_unit))
    if measure_unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("MeasureUnit"), value=measure_unit_prefix))
    if currency and isinstance(currency, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Currency"), value=currency))
    if currency and isinstance(currency, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Currency"), values=currency))
    if currency_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Currency"), value=currency_prefix))
    if min_price or max_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("Price"), gte=min_price, lte=max_price))
    if direction_name and isinstance(direction_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DirectionName"), value=direction_name))
    if direction_name and isinstance(direction_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DirectionName"), values=direction_name))
    if direction_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("DirectionName"), value=direction_name_prefix))
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
