from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopResultOutputTimeSeriesAPI(EdgeAPI):
    def list(
            self,
            from_shop_result: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_result_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_time_series: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_time_series_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List output time series edges of a shop result.

        Args:
            from_shop_result: ID of the source shop result.
            from_shop_result_space: Location of the shop results.
            to_shop_time_series: ID of the target shop time series.
            to_shop_time_series_space: Location of the shop time series.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of output time series edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested output time series edges.

        Examples:

            List 5 output time series edges connected to "my_shop_result":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result = client.shop_result.output_time_series_edge.list("my_shop_result", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopResult.outputTimeSeries"),

            from_shop_result,
            from_shop_result_space,
            to_shop_time_series,
            to_shop_time_series_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
