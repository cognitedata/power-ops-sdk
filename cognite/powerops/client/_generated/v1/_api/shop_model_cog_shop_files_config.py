from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopModelCogShopFilesConfigAPI(EdgeAPI):
    def list(
            self,
            from_shop_model: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_model_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_file: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_file_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List cog shop files config edges of a shop model.

        Args:
            from_shop_model: ID of the source shop model.
            from_shop_model_space: Location of the shop models.
            to_shop_file: ID of the target shop file.
            to_shop_file_space: Location of the shop files.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog shop files config edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested cog shop files config edges.

        Examples:

            List 5 cog shop files config edges connected to "my_shop_model":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model = client.shop_model.cog_shop_files_config_edge.list("my_shop_model", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopModel.cogShopFilesConfig"),

            from_shop_model,
            from_shop_model_space,
            to_shop_file,
            to_shop_file_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
