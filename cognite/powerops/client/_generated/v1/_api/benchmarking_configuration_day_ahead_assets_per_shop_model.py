from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BenchmarkingConfigurationDayAheadAssetsPerShopModelAPI(EdgeAPI):
    def list(
            self,
            from_benchmarking_configuration_day_ahead: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_benchmarking_configuration_day_ahead_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_model_with_asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_model_with_asset_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List assets per shop model edges of a benchmarking configuration day ahead.

        Args:
            from_benchmarking_configuration_day_ahead: ID of the source benchmarking configuration day ahead.
            from_benchmarking_configuration_day_ahead_space: Location of the benchmarking configuration day aheads.
            to_shop_model_with_asset: ID of the target shop model with asset.
            to_shop_model_with_asset_space: Location of the shop model with assets.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets per shop model edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested assets per shop model edges.

        Examples:

            List 5 assets per shop model edges connected to "my_benchmarking_configuration_day_ahead":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_configuration_day_ahead = client.benchmarking_configuration_day_ahead.assets_per_shop_model_edge.list("my_benchmarking_configuration_day_ahead", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "assetsPerShopModel"),

            from_benchmarking_configuration_day_ahead,
            from_benchmarking_configuration_day_ahead_space,
            to_shop_model_with_asset,
            to_shop_model_with_asset_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
