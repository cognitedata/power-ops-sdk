from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import RKOMBid, RKOMBidApply, RKOMBidApplyList, RKOMBidList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class RKOMBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.date"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.date"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in rkom_bid_ids],
            )
            filters.append(is_rkom_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMBidPriceScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.priceScenarios"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.priceScenarios"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in rkom_bid_ids],
            )
            filters.append(is_rkom_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMBidReserveScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.reserveScenarios"},
        )
        if isinstance(external_id, str):
            is_rkom_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid))

        else:
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bids))

    def list(self, rkom_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.reserveScenarios"},
        )
        filters.append(is_edge_type)
        if rkom_bid_id:
            rkom_bid_ids = [rkom_bid_id] if isinstance(rkom_bid_id, str) else rkom_bid_id
            is_rkom_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in rkom_bid_ids],
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
        self.view_id = view_id
        self.date = RKOMBidDateAPI(client)
        self.price_scenarios = RKOMBidPriceScenariosAPI(client)
        self.reserve_scenarios = RKOMBidReserveScenariosAPI(client)

    def apply(self, rkom_bid: RKOMBidApply | Sequence[RKOMBidApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(rkom_bid, RKOMBidApply):
            instances = rkom_bid.to_instances_apply()
        else:
            instances = RKOMBidApplyList(rkom_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMBid | RKOMBidList:
        if isinstance(external_id, str):
            rkom_bid = self._retrieve((self.sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            rkom_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            rkom_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            rkom_bid.reserve_scenarios = [edge.end_node.external_id for edge in reserve_scenario_edges]

            return rkom_bid
        else:
            rkom_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(rkom_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(rkom_bids, price_scenario_edges)
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            self._set_reserve_scenarios(rkom_bids, reserve_scenario_edges)

            return rkom_bids

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
            self.view_id,
            name,
            name_prefix,
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
            date_edges = self.date.list(rkom_bids.as_external_ids(), limit=-1)
            self._set_date(rkom_bids, date_edges)
            price_scenario_edges = self.price_scenarios.list(rkom_bids.as_external_ids(), limit=-1)
            self._set_price_scenarios(rkom_bids, price_scenario_edges)
            reserve_scenario_edges = self.reserve_scenarios.list(rkom_bids.as_external_ids(), limit=-1)
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
