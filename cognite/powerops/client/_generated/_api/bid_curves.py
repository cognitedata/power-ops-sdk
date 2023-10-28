from __future__ import annotations

from collections import defaultdict
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

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class BidCurvesParentAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BidCurves.parent"},
        )
        if isinstance(external_id, str):
            is_bid_curve = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_bid_curve))

        else:
            is_bid_curves = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_bid_curves)
            )

    def list(
        self, bid_curve_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "BidCurves.parent"},
        )
        filters.append(is_edge_type)
        if bid_curve_id:
            bid_curve_ids = [bid_curve_id] if isinstance(bid_curve_id, str) else bid_curve_id
            is_bid_curves = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in bid_curve_ids],
            )
            filters.append(is_bid_curves)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


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
        self.parent = BidCurvesParentAPI(client)

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
            bid_curve = self._retrieve((self._sources.space, external_id))

            parent_edges = self.parent.retrieve(external_id)
            bid_curve.parent = [edge.end_node.external_id for edge in parent_edges]

            return bid_curve
        else:
            bid_curves = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            parent_edges = self.parent.retrieve(external_id)
            self._set_parent(bid_curves, parent_edges)

            return bid_curves

    def search(
        self,
        query: str,
        properties: BidCurvesTextFields | Sequence[BidCurvesTextFields] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidCurvesList:
        filter_ = _create_filter(
            self._view_id,
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
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
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
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
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
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidCurvesList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            filter,
        )

        bid_curves = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := bid_curves.as_external_ids()) > IN_FILTER_LIMIT:
                parent_edges = self.parent.list(limit=-1)
            else:
                parent_edges = self.parent.list(external_ids, limit=-1)
            self._set_parent(bid_curves, parent_edges)

        return bid_curves

    @staticmethod
    def _set_parent(bid_curves: Sequence[BidCurves], parent_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in parent_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for bid_curve in bid_curves:
            node_id = bid_curve.id_tuple()
            if node_id in edges_by_start_node:
                bid_curve.parent = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
