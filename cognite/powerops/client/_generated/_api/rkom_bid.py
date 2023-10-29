from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    RKOMBid,
    RKOMBidApply,
    RKOMBidApplyList,
    RKOMBidFields,
    RKOMBidList,
    RKOMBidTextFields,
)
from cognite.powerops.client._generated.data_classes._rkom_bid import _RKOMBID_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class RKOMBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.date"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(
        self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.date"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in rkom_bid_ids],
            )
            filters.append(is_rkom_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMBidPriceScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.priceScenarios"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(
        self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.priceScenarios"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in rkom_bid_ids],
            )
            filters.append(is_rkom_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMBidReserveScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.reserveScenarios"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(
        self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMBid.reserveScenarios"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in rkom_bid_ids],
            )
            filters.append(is_rkom_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMBidAPI(TypeAPI[RKOMBid, RKOMBidApply, RKOMBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMBid,
            class_apply_type=RKOMBidApply,
            class_list=RKOMBidList,
        )
        self._view_id = view_id
        self.date = RKOMBidDateAPI(client)
        self.price_scenarios = RKOMBidPriceScenariosAPI(client)
        self.reserve_scenarios = RKOMBidReserveScenariosAPI(client)

    def apply(self, rkom_bid: RKOMBidApply | Sequence[RKOMBidApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(rkom_bid, RKOMBidApply):
            instances = rkom_bid.to_instances_apply()
        else:
            instances = RKOMBidApplyList(rkom_bid).to_instances_apply()
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
    def retrieve(self, external_id: str) -> RKOMBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMBid | RKOMBidList:
        if isinstance(external_id, str):
            rkom_bid = self._retrieve((self._sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            rkom_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            rkom_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            rkom_bid.reserve_scenarios = [edge.end_node.external_id for edge in reserve_scenario_edges]

            return rkom_bid
        else:
            rkom_bids = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(rkom_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(rkom_bids, price_scenario_edges)
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            self._set_reserve_scenarios(rkom_bids, reserve_scenario_edges)

            return rkom_bids

    def search(
        self,
        query: str,
        properties: RKOMBidTextFields | Sequence[RKOMBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            method,
            method_prefix,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            watercourse,
            watercourse_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RKOMBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: RKOMBidFields | Sequence[RKOMBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: RKOMBidTextFields | Sequence[RKOMBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
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
        property: RKOMBidFields | Sequence[RKOMBidFields] | None = None,
        group_by: RKOMBidFields | Sequence[RKOMBidFields] = None,
        query: str | None = None,
        search_properties: RKOMBidTextFields | Sequence[RKOMBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
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
        property: RKOMBidFields | Sequence[RKOMBidFields] | None = None,
        group_by: RKOMBidFields | Sequence[RKOMBidFields] | None = None,
        query: str | None = None,
        search_property: RKOMBidTextFields | Sequence[RKOMBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            method,
            method_prefix,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            watercourse,
            watercourse_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RKOMBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: RKOMBidFields,
        interval: float,
        query: str | None = None,
        search_property: RKOMBidTextFields | Sequence[RKOMBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            method,
            method_prefix,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            watercourse,
            watercourse_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RKOMBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> RKOMBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            method,
            method_prefix,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            watercourse,
            watercourse_prefix,
            external_id_prefix,
            filter,
        )

        rkom_bids = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := rkom_bids.as_external_ids()) > IN_FILTER_LIMIT:
                date_edges = self.date.list(limit=-1)
            else:
                date_edges = self.date.list(external_ids, limit=-1)
            self._set_date(rkom_bids, date_edges)
            if len(external_ids := rkom_bids.as_external_ids()) > IN_FILTER_LIMIT:
                price_scenario_edges = self.price_scenarios.list(limit=-1)
            else:
                price_scenario_edges = self.price_scenarios.list(external_ids, limit=-1)
            self._set_price_scenarios(rkom_bids, price_scenario_edges)
            if len(external_ids := rkom_bids.as_external_ids()) > IN_FILTER_LIMIT:
                reserve_scenario_edges = self.reserve_scenarios.list(limit=-1)
            else:
                reserve_scenario_edges = self.reserve_scenarios.list(external_ids, limit=-1)
            self._set_reserve_scenarios(rkom_bids, reserve_scenario_edges)

        return rkom_bids

    @staticmethod
    def _set_date(rkom_bids: Sequence[RKOMBid], date_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in date_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_bid in rkom_bids:
            node_id = rkom_bid.id_tuple()
            if node_id in edges_by_start_node:
                rkom_bid.date = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_price_scenarios(rkom_bids: Sequence[RKOMBid], price_scenario_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in price_scenario_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_bid in rkom_bids:
            node_id = rkom_bid.id_tuple()
            if node_id in edges_by_start_node:
                rkom_bid.price_scenarios = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_reserve_scenarios(rkom_bids: Sequence[RKOMBid], reserve_scenario_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in reserve_scenario_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_bid in rkom_bids:
            node_id = rkom_bid.id_tuple()
            if node_id in edges_by_start_node:
                rkom_bid.reserve_scenarios = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    min_minimum_price: float | None = None,
    max_minimum_price: float | None = None,
    min_price_premium: float | None = None,
    max_price_premium: float | None = None,
    watercourse: str | list[str] | None = None,
    watercourse_prefix: str | None = None,
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
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "power-ops", "externalId": market})
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"),
                values=[{"space": "power-ops", "externalId": item} for item in market],
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if min_minimum_price or max_minimum_price:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("minimumPrice"), gte=min_minimum_price, lte=max_minimum_price)
        )
    if min_price_premium or max_price_premium:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("pricePremium"), gte=min_price_premium, lte=max_price_premium)
        )
    if watercourse and isinstance(watercourse, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("watercourse"), value=watercourse))
    if watercourse and isinstance(watercourse, list):
        filters.append(dm.filters.In(view_id.as_property_ref("watercourse"), values=watercourse))
    if watercourse_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("watercourse"), value=watercourse_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
