from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BenchmarkingConfigurationDayAhead,
    PriceAreaDayAhead,
    DateSpecification,
    DateSpecification,
)
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import (
    BidConfigurationDayAhead,
    _create_bid_configuration_day_ahead_filter,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model_with_assets import (
    ShopModelWithAssets,
    _create_shop_model_with_asset_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .bid_configuration_day_ahead_query import BidConfigurationDayAheadQueryAPI
    from .shop_model_with_assets_query import ShopModelWithAssetsQueryAPI



class BenchmarkingConfigurationDayAheadQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingConfigurationDayAhead", "1")

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
                name=self._builder.next_name("benchmarking_configuration_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=BenchmarkingConfigurationDayAhead,
                max_retrieve_limit=limit,
            )
        )

    def bid_configurations(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        bid_date_specification: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> BidConfigurationDayAheadQueryAPI[T_DomainModelList]:
        """Query along the bid configuration edges of the benchmarking configuration day ahead.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of bid configuration edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the shop start specification for each benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the shop end specification for each benchmarking configuration day ahead or not.

        Returns:
            BidConfigurationDayAheadQueryAPI: The query API for the bid configuration day ahead.
        """
        from .bid_configuration_day_ahead_query import BidConfigurationDayAheadQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "benchmarkingBidConfigurations"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_configurations"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = BidConfigurationDayAheadQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_bid_configuration_day_ahead_filter(
            view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_shop_start_specification:
            self._query_append_shop_start_specification(from_)
        if retrieve_shop_end_specification:
            self._query_append_shop_end_specification(from_)
        return BidConfigurationDayAheadQueryAPI(self._client, self._builder, node_filer, limit)

    def assets_per_shop_model(
        self,
        shop_model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop_commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> ShopModelWithAssetsQueryAPI[T_DomainModelList]:
        """Query along the assets per shop model edges of the benchmarking configuration day ahead.

        Args:
            shop_model: The shop model to filter on.
            shop_commands: The shop command to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of assets per shop model edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the shop start specification for each benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the shop end specification for each benchmarking configuration day ahead or not.

        Returns:
            ShopModelWithAssetsQueryAPI: The query API for the shop model with asset.
        """
        from .shop_model_with_assets_query import ShopModelWithAssetsQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "assetsPerShopModel"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("assets_per_shop_model"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = ShopModelWithAssetsQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_model_with_asset_filter(
            view_id,
            shop_model,
            shop_commands,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_shop_start_specification:
            self._query_append_shop_start_specification(from_)
        if retrieve_shop_end_specification:
            self._query_append_shop_end_specification(from_)
        return ShopModelWithAssetsQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_price_area: Whether to retrieve the price area for each benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the shop start specification for each benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the shop end specification for each benchmarking configuration day ahead or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_shop_start_specification:
            self._query_append_shop_start_specification(from_)
        if retrieve_shop_end_specification:
            self._query_append_shop_end_specification(from_)
        return self._query()

    def _query_append_price_area(self, from_: str) -> None:
        view_id = PriceAreaDayAhead._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("priceArea"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceAreaDayAhead,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_shop_start_specification(self, from_: str) -> None:
        view_id = DateSpecification._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_start_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("shopStartSpecification"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateSpecification,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_shop_end_specification(self, from_: str) -> None:
        view_id = DateSpecification._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_end_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("shopEndSpecification"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateSpecification,
                is_single_direct_relation=True,
            ),
        )
