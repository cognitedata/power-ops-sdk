from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    Periods,
    PeriodsApply,
    PeriodsApplyList,
    PeriodsFields,
    PeriodsList,
    PeriodsTextFields,
)
from cognite.powerops.client._generated.data_classes._periods import _PERIODS_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class PeriodsBidCurvesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ReserveBidTimeSeries.BidCurves"},
        )
        if isinstance(external_id, str):
            is_period = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_period))

        else:
            is_periods = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_periods))

    def list(
        self, period_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "ReserveBidTimeSeries.BidCurves"},
        )
        filters.append(is_edge_type)
        if period_id:
            period_ids = [period_id] if isinstance(period_id, str) else period_id
            is_periods = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in period_ids],
            )
            filters.append(is_periods)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PeriodsAPI(TypeAPI[Periods, PeriodsApply, PeriodsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Periods,
            class_apply_type=PeriodsApply,
            class_list=PeriodsList,
        )
        self._view_id = view_id
        self.bid_curves = PeriodsBidCurvesAPI(client)

    def apply(self, period: PeriodsApply | Sequence[PeriodsApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(period, PeriodsApply):
            instances = period.to_instances_apply()
        else:
            instances = PeriodsApplyList(period).to_instances_apply()
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
    def retrieve(self, external_id: str) -> Periods:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PeriodsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Periods | PeriodsList:
        if isinstance(external_id, str):
            period = self._retrieve((self._sources.space, external_id))

            bid_curve_edges = self.bid_curves.retrieve(external_id)
            period.bid_curves = [edge.end_node.external_id for edge in bid_curve_edges]

            return period
        else:
            periods = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            bid_curve_edges = self.bid_curves.retrieve(external_id)
            self._set_bid_curves(periods, bid_curve_edges)

            return periods

    def search(
        self,
        query: str,
        properties: PeriodsTextFields | Sequence[PeriodsTextFields] | None = None,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PeriodsList:
        filter_ = _create_filter(
            self._view_id,
            resolution,
            resolution_prefix,
            time_interval,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _PERIODS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PeriodsFields | Sequence[PeriodsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PeriodsTextFields | Sequence[PeriodsTextFields] | None = None,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PeriodsFields | Sequence[PeriodsFields] | None = None,
        group_by: PeriodsFields | Sequence[PeriodsFields] = None,
        query: str | None = None,
        search_properties: PeriodsTextFields | Sequence[PeriodsTextFields] | None = None,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PeriodsFields | Sequence[PeriodsFields] | None = None,
        group_by: PeriodsFields | Sequence[PeriodsFields] | None = None,
        query: str | None = None,
        search_property: PeriodsTextFields | Sequence[PeriodsTextFields] | None = None,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            resolution,
            resolution_prefix,
            time_interval,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PERIODS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PeriodsFields,
        interval: float,
        query: str | None = None,
        search_property: PeriodsTextFields | Sequence[PeriodsTextFields] | None = None,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            resolution,
            resolution_prefix,
            time_interval,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PERIODS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        resolution: str | list[str] | None = None,
        resolution_prefix: str | None = None,
        time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PeriodsList:
        filter_ = _create_filter(
            self._view_id,
            resolution,
            resolution_prefix,
            time_interval,
            external_id_prefix,
            filter,
        )

        periods = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := periods.as_external_ids()) > IN_FILTER_LIMIT:
                bid_curve_edges = self.bid_curves.list(limit=-1)
            else:
                bid_curve_edges = self.bid_curves.list(external_ids, limit=-1)
            self._set_bid_curves(periods, bid_curve_edges)

        return periods

    @staticmethod
    def _set_bid_curves(periods: Sequence[Periods], bid_curve_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in bid_curve_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for period in periods:
            node_id = period.id_tuple()
            if node_id in edges_by_start_node:
                period.bid_curves = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    resolution: str | list[str] | None = None,
    resolution_prefix: str | None = None,
    time_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resolution and isinstance(resolution, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Resolution"), value=resolution))
    if resolution and isinstance(resolution, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Resolution"), values=resolution))
    if resolution_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Resolution"), value=resolution_prefix))
    if time_interval and isinstance(time_interval, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("TimeInterval"), value={"space": "power-ops", "externalId": time_interval}
            )
        )
    if time_interval and isinstance(time_interval, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("TimeInterval"),
                value={"space": time_interval[0], "externalId": time_interval[1]},
            )
        )
    if time_interval and isinstance(time_interval, list) and isinstance(time_interval[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("TimeInterval"),
                values=[{"space": "power-ops", "externalId": item} for item in time_interval],
            )
        )
    if time_interval and isinstance(time_interval, list) and isinstance(time_interval[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("TimeInterval"),
                values=[{"space": item[0], "externalId": item[1]} for item in time_interval],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
