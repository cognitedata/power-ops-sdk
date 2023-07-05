from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market_configuration.data_classes import (
    RKOMBidCombination,
    RKOMBidCombinationApply,
    RKOMBidCombinationList,
)

from ._core import TypeAPI


class RKOMBidCombinationConfigurationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBidCombination.bidConfigurations"},
        )
        if isinstance(external_id, str):
            is_rkom_bid_combination = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid_combination)
            )

        else:
            is_rkom_bid_combinations = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_bid_combinations)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "RKOMBidCombination.bidConfigurations"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class RKOMBidCombinationsAPI(TypeAPI[RKOMBidCombination, RKOMBidCombinationApply, RKOMBidCombinationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "RKOMBidCombination", "79f6abad4b6c04"),
            class_type=RKOMBidCombination,
            class_apply_type=RKOMBidCombinationApply,
            class_list=RKOMBidCombinationList,
        )
        self.bid_configurations = RKOMBidCombinationConfigurationsAPI(client)

    def apply(self, rkom_bid_combination: RKOMBidCombinationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = rkom_bid_combination.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RKOMBidCombinationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RKOMBidCombinationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> RKOMBidCombination:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMBidCombinationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMBidCombination | RKOMBidCombinationList:
        if isinstance(external_id, str):
            rkom_bid_combination = self._retrieve((self.sources.space, external_id))

            bid_configuration_edges = self.bid_configurations.retrieve(external_id)
            rkom_bid_combination.bid_configurations = [edge.end_node.external_id for edge in bid_configuration_edges]

            return rkom_bid_combination
        else:
            rkom_bid_combinations = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            bid_configuration_edges = self.bid_configurations.retrieve(external_id)
            self._set_bid_configurations(rkom_bid_combinations, bid_configuration_edges)

            return rkom_bid_combinations

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> RKOMBidCombinationList:
        rkom_bid_combinations = self._list(limit=limit)

        bid_configuration_edges = self.bid_configurations.list(limit=-1)
        self._set_bid_configurations(rkom_bid_combinations, bid_configuration_edges)

        return rkom_bid_combinations

    @staticmethod
    def _set_bid_configurations(
        rkom_bid_combinations: Sequence[RKOMBidCombination], bid_configuration_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in bid_configuration_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_bid_combination in rkom_bid_combinations:
            node_id = rkom_bid_combination.id_tuple()
            if node_id in edges_by_start_node:
                rkom_bid_combination.bid_configurations = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]
