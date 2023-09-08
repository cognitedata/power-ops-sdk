from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import RKOMBid, RKOMBidApply, RKOMBidList


class RKOMBidDatesAPI:
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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.date"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.priceScenarios"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBid.reserveScenarios"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class RKOMBidsAPI(TypeAPI[RKOMBid, RKOMBidApply, RKOMBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMBid", "5c1a2ba06aa41f"),
            class_type=RKOMBid,
            class_apply_type=RKOMBidApply,
            class_list=RKOMBidList,
        )
        self.dates = RKOMBidDatesAPI(client)
        self.price_scenarios = RKOMBidPriceScenariosAPI(client)
        self.reserve_scenarios = RKOMBidReserveScenariosAPI(client)

    def apply(self, rkom_bid: RKOMBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_bid.to_instances_apply()
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

            date_edges = self.dates.retrieve(external_id)
            rkom_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            rkom_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            rkom_bid.reserve_scenarios = [edge.end_node.external_id for edge in reserve_scenario_edges]

            return rkom_bid
        else:
            rkom_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.dates.retrieve(external_id)
            self._set_date(rkom_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(rkom_bids, price_scenario_edges)
            reserve_scenario_edges = self.reserve_scenarios.retrieve(external_id)
            self._set_reserve_scenarios(rkom_bids, reserve_scenario_edges)

            return rkom_bids

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> RKOMBidList:
        rkom_bids = self._list(limit=limit)

        date_edges = self.dates.list(limit=-1)
        self._set_date(rkom_bids, date_edges)
        price_scenario_edges = self.price_scenarios.list(limit=-1)
        self._set_price_scenarios(rkom_bids, price_scenario_edges)
        reserve_scenario_edges = self.reserve_scenarios.list(limit=-1)
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
