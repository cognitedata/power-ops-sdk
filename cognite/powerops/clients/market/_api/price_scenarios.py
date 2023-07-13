from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.market.data_classes import PriceScenario, PriceScenarioApply, PriceScenarioList

from ._core import TypeAPI


class PriceScenarioTransformationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceScenario.transformations"},
        )
        if isinstance(external_id, str):
            is_price_scenario = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_scenario)
            )

        else:
            is_price_scenarios = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_scenarios)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceScenario.transformations"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PriceScenariosAPI(TypeAPI[PriceScenario, PriceScenarioApply, PriceScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "PriceScenario", "ab0a97e32180e1"),
            class_type=PriceScenario,
            class_apply_type=PriceScenarioApply,
            class_list=PriceScenarioList,
        )
        self.transformations = PriceScenarioTransformationsAPI(client)

    def apply(self, price_scenario: PriceScenarioApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = price_scenario.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PriceScenarioApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PriceScenarioApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PriceScenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PriceScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PriceScenario | PriceScenarioList:
        if isinstance(external_id, str):
            price_scenario = self._retrieve((self.sources.space, external_id))

            transformation_edges = self.transformations.retrieve(external_id)
            price_scenario.transformations = [edge.end_node.external_id for edge in transformation_edges]

            return price_scenario
        else:
            price_scenarios = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            transformation_edges = self.transformations.retrieve(external_id)
            self._set_transformations(price_scenarios, transformation_edges)

            return price_scenarios

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> PriceScenarioList:
        price_scenarios = self._list(limit=limit)

        transformation_edges = self.transformations.list(limit=-1)
        self._set_transformations(price_scenarios, transformation_edges)

        return price_scenarios

    @staticmethod
    def _set_transformations(price_scenarios: Sequence[PriceScenario], transformation_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in transformation_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for price_scenario in price_scenarios:
            node_id = price_scenario.id_tuple()
            if node_id in edges_by_start_node:
                price_scenario.transformations = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
