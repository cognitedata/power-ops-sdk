from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopModelWithAssets,
    ShopModelWithAssetsWrite,
    ShopModelWithAssetsList,
    ShopModelWithAssetsWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model_with_assets import (
    _create_shop_model_with_asset_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .shop_model_with_assets_power_assets import ShopModelWithAssetsPowerAssetsAPI
from .shop_model_with_assets_production_obligations import ShopModelWithAssetsProductionObligationsAPI
from .shop_model_with_assets_query import ShopModelWithAssetsQueryAPI


class ShopModelWithAssetsAPI(NodeAPI[ShopModelWithAssets, ShopModelWithAssetsWrite, ShopModelWithAssetsList, ShopModelWithAssetsWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")
    _properties_by_field = {}
    _class_type = ShopModelWithAssets
    _class_list = ShopModelWithAssetsList
    _class_write_list = ShopModelWithAssetsWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.power_assets_edge = ShopModelWithAssetsPowerAssetsAPI(client)
        self.production_obligations_edge = ShopModelWithAssetsProductionObligationsAPI(client)

    def __call__(
            self,
            shop_model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            shop_commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> ShopModelWithAssetsQueryAPI[ShopModelWithAssetsList]:
        """Query starting at shop model with assets.

        Args:
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop model with assets.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopModelWithAssetsList)
        return ShopModelWithAssetsQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        shop_model_with_asset: ShopModelWithAssetsWrite | Sequence[ShopModelWithAssetsWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop model with assets.

        Note: This method iterates through all nodes and timeseries linked to shop_model_with_asset and creates them including the edges
        between the nodes. For example, if any of `power_assets` or `production_obligations` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_model_with_asset: Shop model with asset or sequence of shop model with assets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_model_with_asset:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopModelWithAssetsWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = ShopModelWithAssetsWrite(external_id="my_shop_model_with_asset", ...)
                >>> result = client.shop_model_with_assets.apply(shop_model_with_asset)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_model_with_assets.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_model_with_asset, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop model with asset.

        Args:
            external_id: External id of the shop model with asset to delete.
            space: The space where all the shop model with asset are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_model_with_asset by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_model_with_assets.delete("my_shop_model_with_asset")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_model_with_assets.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ShopModelWithAssets | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopModelWithAssetsList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopModelWithAssets | ShopModelWithAssetsList | None:
        """Retrieve one or more shop model with assets by id(s).

        Args:
            external_id: External id or list of external ids of the shop model with assets.
            space: The space where all the shop model with assets are located.

        Returns:
            The requested shop model with assets.

        Examples:

            Retrieve shop_model_with_asset by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = client.shop_model_with_assets.retrieve("my_shop_model_with_asset")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.power_assets_edge,
                    "power_assets",
                    dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PowerAsset", "1"),
                ),
                (
                    self.production_obligations_edge,
                    "production_obligations",
                    dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
                    "outwards",
                    dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1"),
                ),
                                               ]
        )




    def list(
        self,
        shop_model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop_commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ShopModelWithAssetsList:
        """List/filter shop model with assets

        Args:
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `power_assets` or `production_obligations` external ids for the shop model with assets. Defaults to True.

        Returns:
            List of requested shop model with assets

        Examples:

            List shop model with assets and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_assets = client.shop_model_with_assets.list(limit=5)

        """
        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.power_assets_edge,
                    "power_assets",
                    dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PowerAsset", "1"),
                ),
                (
                    self.production_obligations_edge,
                    "production_obligations",
                    dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
                    "outwards",
                    dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1"),
                ),
                                               ]
        )
