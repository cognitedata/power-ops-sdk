from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market_configuration.data_classes import Bid, BidApply, BidList

from ._core import TypeAPI


class BidDatesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Bid.date"},
        )
        if isinstance(external_id, str):
            is_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_bid))

        else:
            is_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_bids))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Bid.date"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class BidsAPI(TypeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Bid", "2155e107f4fd23"),
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
        )
        self.dates = BidDatesAPI(client)

    def apply(self, bid: BidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Bid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Bid | BidList:
        if isinstance(external_id, str):
            bid = self._retrieve((self.sources.space, external_id))

            date_edges = self.dates.retrieve(external_id)
            bid.date = [edge.end_node.external_id for edge in date_edges]

            return bid
        else:
            bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.dates.retrieve(external_id)
            self._set_date(bids, date_edges)

            return bids

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BidList:
        bids = self._list(limit=limit)

        date_edges = self.dates.list(limit=-1)
        self._set_date(bids, date_edges)

        return bids

    @staticmethod
    def _set_date(bids: Sequence[Bid], date_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in date_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for bid in bids:
            node_id = bid.id_tuple()
            if node_id in edges_by_start_node:
                bid.date = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
