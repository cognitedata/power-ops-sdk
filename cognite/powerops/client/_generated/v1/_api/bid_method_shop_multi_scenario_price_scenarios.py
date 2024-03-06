from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidMethodSHOPMultiScenarioPriceScenariosAPI(EdgeAPI):
    def list(
        self,
        from_bid_method_shop_multi_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_bid_method_shop_multi_scenario_space: str = DEFAULT_INSTANCE_SPACE,
        to_mapping: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_mapping_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List price scenario edges of a bid method shop multi scenario.

        Args:
            from_bid_method_shop_multi_scenario: ID of the source bid method shop multi scenario.
            from_bid_method_shop_multi_scenario_space: Location of the bid method shop multi scenarios.
            to_mapping: ID of the target mapping.
            to_mapping_space: Location of the mappings.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price scenario edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested price scenario edges.

        Examples:

            List 5 price scenario edges connected to "my_bid_method_shop_multi_scenario":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_shop_multi_scenario = client.bid_method_shop_multi_scenario.price_scenarios_edge.list("my_bid_method_shop_multi_scenario", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidMethodDayahead.priceScenarios"),
            from_bid_method_shop_multi_scenario,
            from_bid_method_shop_multi_scenario_space,
            to_mapping,
            to_mapping_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
