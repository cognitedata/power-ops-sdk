from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE


class SHOPTableProductionPricePairsAPI(EdgeAPI):
    def list(
        self,
        from_shop_table: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_shop_table_space: str = DEFAULT_INSTANCE_SPACE,
        to_production_price_pair: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_production_price_pair_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List production price pair edges of a shop table.

        Args:
            from_shop_table: ID of the source shop table.
            from_shop_table_space: Location of the shop tables.
            to_production_price_pair: ID of the target production price pair.
            to_production_price_pair_space: Location of the production price pairs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pair edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested production price pair edges.

        Examples:

            List 5 production price pair edges connected to "my_shop_table":

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_table = client.shop_table.production_price_pairs_edge.list("my_shop_table", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "SHOPTable.productionPricePairs"),
            from_shop_table,
            from_shop_table_space,
            to_production_price_pair,
            to_production_price_pair_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
