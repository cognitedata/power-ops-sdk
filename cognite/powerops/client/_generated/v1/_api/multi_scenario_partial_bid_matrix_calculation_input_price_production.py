from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class MultiScenarioPartialBidMatrixCalculationInputPriceProductionAPI(EdgeAPI):
    def list(
            self,
            from_multi_scenario_partial_bid_matrix_calculation_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_multi_scenario_partial_bid_matrix_calculation_input_space: str = DEFAULT_INSTANCE_SPACE,
            to_price_production: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_price_production_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List price production edges of a multi scenario partial bid matrix calculation input.

        Args:
            from_multi_scenario_partial_bid_matrix_calculation_input: ID of the source multi scenario partial bid matrix calculation input.
            from_multi_scenario_partial_bid_matrix_calculation_input_space: Location of the multi scenario partial bid matrix calculation inputs.
            to_price_production: ID of the target price production.
            to_price_production_space: Location of the price productions.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price production edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested price production edges.

        Examples:

            List 5 price production edges connected to "my_multi_scenario_partial_bid_matrix_calculation_input":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_partial_bid_matrix_calculation_input = client.multi_scenario_partial_bid_matrix_calculation_input.price_production_edge.list("my_multi_scenario_partial_bid_matrix_calculation_input", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "PriceProduction"),

            from_multi_scenario_partial_bid_matrix_calculation_input,
            from_multi_scenario_partial_bid_matrix_calculation_input_space,
            to_price_production,
            to_price_production_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
