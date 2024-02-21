from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE


class MultiScenarioMatrixScenarioResultsAPI(EdgeAPI):
    def list(
        self,
        from_multi_scenario_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_multi_scenario_matrix_space: str = DEFAULT_INSTANCE_SPACE,
        to_shop_price_scenario_result: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_shop_price_scenario_result_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List scenario result edges of a multi scenario matrix.

        Args:
            from_multi_scenario_matrix: ID of the source multi scenario matrix.
            from_multi_scenario_matrix_space: Location of the multi scenario matrixes.
            to_shop_price_scenario_result: ID of the target shop price scenario result.
            to_shop_price_scenario_result_space: Location of the shop price scenario results.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario result edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested scenario result edges.

        Examples:

            List 5 scenario result edges connected to "my_multi_scenario_matrix":

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> multi_scenario_matrix = client.multi_scenario_matrix.scenario_results_edge.list("my_multi_scenario_matrix", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "scenarioResult"),
            from_multi_scenario_matrix,
            from_multi_scenario_matrix_space,
            to_shop_price_scenario_result,
            to_shop_price_scenario_result_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
