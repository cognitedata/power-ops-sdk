from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopModelWithAssets,
    ShopModel,
    ShopCommands,
)
from cognite.powerops.client._generated.v1.data_classes._power_asset import (
    PowerAsset,
    _create_power_asset_filter,
)
from cognite.powerops.client._generated.v1.data_classes._benchmarking_production_obligation_day_ahead import (
    BenchmarkingProductionObligationDayAhead,
    _create_benchmarking_production_obligation_day_ahead_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.power_asset_query import PowerAssetQueryAPI
    from cognite.powerops.client._generated.v1._api.benchmarking_production_obligation_day_ahead_query import BenchmarkingProductionObligationDayAheadQueryAPI


class ShopModelWithAssetsQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=ShopModelWithAssets,
                max_retrieve_limit=limit,
            )
        )
    def power_assets(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_shop_model: bool = False,
        retrieve_shop_commands: bool = False,
    ) -> PowerAssetQueryAPI[T_DomainModelList]:
        """Query along the power asset edges of the shop model with asset.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of power asset edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_shop_model: Whether to retrieve the shop model for each shop model with asset or not.
            retrieve_shop_commands: Whether to retrieve the shop command for each shop model with asset or not.

        Returns:
            PowerAssetQueryAPI: The query API for the power asset.
        """
        from .power_asset_query import PowerAssetQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = PowerAssetQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_power_asset_filter(
            view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_shop_model:
            self._query_append_shop_model(from_)
        if retrieve_shop_commands:
            self._query_append_shop_commands(from_)
        return PowerAssetQueryAPI(self._client, self._builder, node_filer, limit)
    def production_obligations(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_shop_model: bool = False,
        retrieve_shop_commands: bool = False,
    ) -> BenchmarkingProductionObligationDayAheadQueryAPI[T_DomainModelList]:
        """Query along the production obligation edges of the shop model with asset.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of production obligation edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_shop_model: Whether to retrieve the shop model for each shop model with asset or not.
            retrieve_shop_commands: Whether to retrieve the shop command for each shop model with asset or not.

        Returns:
            BenchmarkingProductionObligationDayAheadQueryAPI: The query API for the benchmarking production obligation day ahead.
        """
        from .benchmarking_production_obligation_day_ahead_query import BenchmarkingProductionObligationDayAheadQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = BenchmarkingProductionObligationDayAheadQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_benchmarking_production_obligation_day_ahead_filter(
            view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_shop_model:
            self._query_append_shop_model(from_)
        if retrieve_shop_commands:
            self._query_append_shop_commands(from_)
        return BenchmarkingProductionObligationDayAheadQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_shop_model: bool = False,
        retrieve_shop_commands: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_shop_model: Whether to retrieve the shop model for each shop model with asset or not.
            retrieve_shop_commands: Whether to retrieve the shop command for each shop model with asset or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_shop_model:
            self._query_append_shop_model(from_)
        if retrieve_shop_commands:
            self._query_append_shop_commands(from_)
        return self._query()

    def _query_append_shop_model(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("shopModel"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopModel._view_id]),
                ),
                result_cls=ShopModel,
            ),
        )
    def _query_append_shop_commands(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("shopCommands"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopCommands._view_id]),
                ),
                result_cls=ShopCommands,
            ),
        )
