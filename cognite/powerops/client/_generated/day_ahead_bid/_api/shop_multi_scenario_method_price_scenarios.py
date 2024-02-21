from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE


class SHOPMultiScenarioMethodPriceScenariosAPI(EdgeAPI):
    def list(
        self,
        from_shop_multi_scenario_method: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_shop_multi_scenario_method_space: str = DEFAULT_INSTANCE_SPACE,
        to_shop_price_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_shop_price_scenario_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List price scenario edges of a shop multi scenario method.

        Args:
            from_shop_multi_scenario_method: ID of the source shop multi scenario method.
            from_shop_multi_scenario_method_space: Location of the shop multi scenario methods.
            to_shop_price_scenario: ID of the target shop price scenario.
            to_shop_price_scenario_space: Location of the shop price scenarios.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price scenario edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested price scenario edges.

        Examples:

            List 5 price scenario edges connected to "my_shop_multi_scenario_method":

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario_method = client.shop_multi_scenario_method.price_scenarios_edge.list("my_shop_multi_scenario_method", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "PriceScenario"),
            from_shop_multi_scenario_method,
            from_shop_multi_scenario_method_space,
            to_shop_price_scenario,
            to_shop_price_scenario_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
