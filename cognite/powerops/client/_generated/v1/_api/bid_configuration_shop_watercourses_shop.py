from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidConfigurationShopWatercoursesShopAPI(EdgeAPI):
    def list(
        self,
        from_bid_configuration_shop: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_bid_configuration_shop_space: str = DEFAULT_INSTANCE_SPACE,
        to_watercourse_shop: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_watercourse_shop_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List watercourses shop edges of a bid configuration shop.

        Args:
            from_bid_configuration_shop: ID of the source bid configuration shop.
            from_bid_configuration_shop_space: Location of the bid configuration shops.
            to_watercourse_shop: ID of the target watercourse shop.
            to_watercourse_shop_space: Location of the watercourse shops.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses shop edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested watercourses shop edges.

        Examples:

            List 5 watercourses shop edges connected to "my_bid_configuration_shop":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_shop = client.bid_configuration_shop.watercourses_shop_edge.list("my_bid_configuration_shop", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercoursesShop"),
            from_bid_configuration_shop,
            from_bid_configuration_shop_space,
            to_watercourse_shop,
            to_watercourse_shop_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
