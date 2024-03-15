from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class MultiScenarioMatrixRawShopResultsAPI(EdgeAPI):
    def list(
        self,
        from_multi_scenario_matrix_raw: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_multi_scenario_matrix_raw_space: str = DEFAULT_INSTANCE_SPACE,
        to_shop_result_price_prod: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_shop_result_price_prod_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List shop result edges of a multi scenario matrix raw.

        Args:
            from_multi_scenario_matrix_raw: ID of the source multi scenario matrix raw.
            from_multi_scenario_matrix_raw_space: Location of the multi scenario matrix raws.
            to_shop_result_price_prod: ID of the target shop result price prod.
            to_shop_result_price_prod_space: Location of the shop result price prods.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested shop result edges.

        Examples:

            List 5 shop result edges connected to "my_multi_scenario_matrix_raw":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_matrix_raw = client.multi_scenario_matrix_raw.shop_results_edge.list("my_multi_scenario_matrix_raw", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "MultiScenarioMatrix.shopResults"),
            from_multi_scenario_matrix_raw,
            from_multi_scenario_matrix_raw_space,
            to_shop_result_price_prod,
            to_shop_result_price_prod_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
