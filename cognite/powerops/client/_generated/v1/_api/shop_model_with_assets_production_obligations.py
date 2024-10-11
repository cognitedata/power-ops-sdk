from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopModelWithAssetsProductionObligationsAPI(EdgeAPI):
    def list(
            self,
            from_shop_model_with_asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_model_with_asset_space: str = DEFAULT_INSTANCE_SPACE,
            to_benchmarking_production_obligation_day_ahead: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_benchmarking_production_obligation_day_ahead_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List production obligation edges of a shop model with asset.

        Args:
            from_shop_model_with_asset: ID of the source shop model with asset.
            from_shop_model_with_asset_space: Location of the shop model with assets.
            to_benchmarking_production_obligation_day_ahead: ID of the target benchmarking production obligation day ahead.
            to_benchmarking_production_obligation_day_ahead_space: Location of the benchmarking production obligation day aheads.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production obligation edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested production obligation edges.

        Examples:

            List 5 production obligation edges connected to "my_shop_model_with_asset":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = client.shop_model_with_assets.production_obligations_edge.list("my_shop_model_with_asset", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),

            from_shop_model_with_asset,
            from_shop_model_with_asset_space,
            to_benchmarking_production_obligation_day_ahead,
            to_benchmarking_production_obligation_day_ahead_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
