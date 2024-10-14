from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

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
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .power_asset_query import PowerAssetQueryAPI
    from .benchmarking_production_obligation_day_ahead_query import BenchmarkingProductionObligationDayAheadQueryAPI



class ShopModelWithAssetsQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_model_with_asset"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
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

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("power_assets"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
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

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("production_obligations"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
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
        view_id = ShopModel._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_model"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("shopModel"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopModel,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_shop_commands(self, from_: str) -> None:
        view_id = ShopCommands._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_commands"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("shopCommands"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopCommands,
                is_single_direct_relation=True,
            ),
        )
