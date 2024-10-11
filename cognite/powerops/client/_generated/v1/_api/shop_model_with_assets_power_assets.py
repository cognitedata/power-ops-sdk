from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopModelWithAssetsPowerAssetsAPI(EdgeAPI):
    def list(
            self,
            from_shop_model_with_asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_model_with_asset_space: str = DEFAULT_INSTANCE_SPACE,
            to_power_asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_power_asset_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List power asset edges of a shop model with asset.

        Args:
            from_shop_model_with_asset: ID of the source shop model with asset.
            from_shop_model_with_asset_space: Location of the shop model with assets.
            to_power_asset: ID of the target power asset.
            to_power_asset_space: Location of the power assets.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested power asset edges.

        Examples:

            List 5 power asset edges connected to "my_shop_model_with_asset":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = client.shop_model_with_assets.power_assets_edge.list("my_shop_model_with_asset", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),

            from_shop_model_with_asset,
            from_shop_model_with_asset_space,
            to_power_asset,
            to_power_asset_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
