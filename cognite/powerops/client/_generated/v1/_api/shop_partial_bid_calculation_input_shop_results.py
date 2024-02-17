from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopPartialBidCalculationInputShopResultsAPI(EdgeAPI):
    def list(
        self,
        from_shop_partial_bid_calculation_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_shop_partial_bid_calculation_input_space: str = DEFAULT_INSTANCE_SPACE,
        to_shop_result: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_shop_result_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List shop result edges of a shop partial bid calculation input.

        Args:
            from_shop_partial_bid_calculation_input: ID of the source shop partial bid calculation input.
            from_shop_partial_bid_calculation_input_space: Location of the shop partial bid calculation inputs.
            to_shop_result: ID of the target shop result.
            to_shop_result_space: Location of the shop results.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested shop result edges.

        Examples:

            List 5 shop result edges connected to "my_shop_partial_bid_calculation_input":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_partial_bid_calculation_input = client.shop_partial_bid_calculation_input.shop_results_edge.list("my_shop_partial_bid_calculation_input", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "SHOPResult"),
            from_shop_partial_bid_calculation_input,
            from_shop_partial_bid_calculation_input_space,
            to_shop_result,
            to_shop_result_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
