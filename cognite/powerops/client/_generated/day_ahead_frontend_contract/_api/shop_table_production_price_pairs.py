from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class SHOPTableProductionPricePairsAPI(EdgeAPI):
    def list(
        self,
        shop_table: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        shop_table_space: str = "power-ops-day-ahead-frontend-contract-model",
        production_price_pair: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        production_price_pair_space: str = "power-ops-day-ahead-frontend-contract-model",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List production price pair edges of a shop table.

        Args:
            shop_table: ID of the source shop tables.
            shop_table_space: Location of the shop tables.
            production_price_pair: ID of the target production price pairs.
            production_price_pair_space: Location of the production price pairs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pair edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested production price pair edges.

        Examples:

            List 5 production price pair edges connected to "my_shop_table":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_table = client.shop_table.production_price_pairs_edge.list("my_shop_table", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-day-ahead-frontend-contract-model", "SHOPTable.productionPricePairs"),
            shop_table,
            shop_table_space,
            production_price_pair,
            production_price_pair_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
