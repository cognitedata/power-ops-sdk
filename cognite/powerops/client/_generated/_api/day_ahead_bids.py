from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import DayAheadBid, DayAheadBidApply, DayAheadBidList


class DayAheadBidDatesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.date"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.date"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class DayAheadBidPriceScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.priceScenarios"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.priceScenarios"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class DayAheadBidsAPI(TypeAPI[DayAheadBid, DayAheadBidApply, DayAheadBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "DayAheadBid", "bd0768f04d3708"),
            class_type=DayAheadBid,
            class_apply_type=DayAheadBidApply,
            class_list=DayAheadBidList,
        )
        self.dates = DayAheadBidDatesAPI(client)
        self.price_scenarios = DayAheadBidPriceScenariosAPI(client)

    def apply(self, day_ahead_bid: DayAheadBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = day_ahead_bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadBid | DayAheadBidList:
        if isinstance(external_id, str):
            day_ahead_bid = self._retrieve((self.sources.space, external_id))

            date_edges = self.dates.retrieve(external_id)
            day_ahead_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            day_ahead_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]

            return day_ahead_bid
        else:
            day_ahead_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.dates.retrieve(external_id)
            self._set_date(day_ahead_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(day_ahead_bids, price_scenario_edges)

            return day_ahead_bids

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> DayAheadBidList:
        day_ahead_bids = self._list(limit=limit)

        date_edges = self.dates.list(limit=-1)
        self._set_date(day_ahead_bids, date_edges)
        price_scenario_edges = self.price_scenarios.list(limit=-1)
        self._set_price_scenarios(day_ahead_bids, price_scenario_edges)

        return day_ahead_bids

    @staticmethod
    def _set_date(day_ahead_bids: Sequence[DayAheadBid], date_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in date_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_bid in day_ahead_bids:
            node_id = day_ahead_bid.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_bid.date = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_price_scenarios(day_ahead_bids: Sequence[DayAheadBid], price_scenario_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in price_scenario_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_bid in day_ahead_bids:
            node_id = day_ahead_bid.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_bid.price_scenarios = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
