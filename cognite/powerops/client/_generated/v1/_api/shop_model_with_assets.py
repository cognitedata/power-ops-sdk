from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model_with_assets import (
    ShopModelWithAssetsQuery,
    _create_shop_model_with_asset_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopModelWithAssets,
    ShopModelWithAssetsWrite,
    ShopModelWithAssetsFields,
    ShopModelWithAssetsList,
    ShopModelWithAssetsWriteList,
    ShopModelWithAssetsTextFields,
    BenchmarkingProductionObligationDayAhead,
    PowerAsset,
    ShopCommands,
    ShopModel,
)
from cognite.powerops.client._generated.v1._api.shop_model_with_assets_power_assets import ShopModelWithAssetsPowerAssetsAPI
from cognite.powerops.client._generated.v1._api.shop_model_with_assets_production_obligations import ShopModelWithAssetsProductionObligationsAPI
from cognite.powerops.client._generated.v1._api.shop_model_with_assets_query import ShopModelWithAssetsQueryAPI


class ShopModelWithAssetsAPI(NodeAPI[ShopModelWithAssets, ShopModelWithAssetsWrite, ShopModelWithAssetsList, ShopModelWithAssetsWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = ShopModelWithAssets
    _class_list = ShopModelWithAssetsList
    _class_write_list = ShopModelWithAssetsWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.power_assets_edge = ShopModelWithAssetsPowerAssetsAPI(client)
        self.production_obligations_edge = ShopModelWithAssetsProductionObligationsAPI(client)

    def __call__(
        self,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopModelWithAssetsQueryAPI[ShopModelWithAssets, ShopModelWithAssetsList]:
        """Query starting at shop model with assets.

        Args:
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop model with assets.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ShopModelWithAssetsQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        shop_model_with_asset: ShopModelWithAssetsWrite | Sequence[ShopModelWithAssetsWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop model with assets.

        Args:
            shop_model_with_asset: Shop model with asset or
                sequence of shop model with assets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_model_with_asset:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopModelWithAssetsWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = ShopModelWithAssetsWrite(
                ...     external_id="my_shop_model_with_asset", ...
                ... )
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
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelWithAssets | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelWithAssetsList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelWithAssets | ShopModelWithAssetsList | None:
        """Retrieve one or more shop model with assets by id(s).

        Args:
            external_id: External id or list of external ids of the shop model with assets.
            space: The space where all the shop model with assets are located.
            retrieve_connections: Whether to retrieve `shop_model`, `shop_commands`, `power_assets` and
            `production_obligations` for the shop model with assets. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            The requested shop model with assets.

        Examples:

            Retrieve shop_model_with_asset by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_asset = client.shop_model_with_assets.retrieve(
                ...     "my_shop_model_with_asset"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: ShopModelWithAssetsTextFields | SequenceNotStr[ShopModelWithAssetsTextFields] | None = None,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopModelWithAssetsList:
        """Search shop model with assets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop model with assets matching the query.

        Examples:

           Search for 'my_shop_model_with_asset' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model_with_assets = client.shop_model_with_assets.search(
                ...     'my_shop_model_with_asset'
                ... )

        """
        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields],
        property: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        property: ShopModelWithAssetsFields | SequenceNotStr[ShopModelWithAssetsFields] | None = None,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop model with assets

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop model with assets in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_model_with_assets.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: ShopModelWithAssetsFields,
        interval: float,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop model with assets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_model_with_asset_filter(
            self._view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> ShopModelWithAssetsQuery:
        """Start selecting from shop model with assets."""
        return ShopModelWithAssetsQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            limit=limit,
            has_container_fields=True,
        ))
        builder.extend(
            factory.from_edge(
                PowerAsset._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "powerAssets"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                BenchmarkingProductionObligationDayAhead._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "productionObligations"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    ShopModel._view_id,
                    ViewPropertyId(self._view_id, "shopModel"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    ShopCommands._view_id,
                    ViewPropertyId(self._view_id, "shopCommands"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()


    def list(
        self,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelWithAssetsList:
        """List/filter shop model with assets

        Args:
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop model with assets to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `shop_model`, `shop_commands`, `power_assets` and
            `production_obligations` for the shop model with assets. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

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
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_)
        values = self._query(filter_, limit, retrieve_connections)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
