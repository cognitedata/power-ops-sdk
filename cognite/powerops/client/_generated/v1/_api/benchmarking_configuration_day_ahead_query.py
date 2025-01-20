from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BenchmarkingConfigurationDayAhead,
    PriceAreaDayAhead,
    DateSpecification,
    DateSpecification,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import (
    _create_bid_configuration_day_ahead_filter,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model_with_assets import (
    _create_shop_model_with_asset_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead_query import BidConfigurationDayAheadQueryAPI
    from cognite.powerops.client._generated.v1._api.shop_model_with_assets_query import ShopModelWithAssetsQueryAPI


class BenchmarkingConfigurationDayAheadQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingConfigurationDayAhead", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )
    def bid_configurations(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> BidConfigurationDayAheadQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the bid configuration edges of the benchmarking configuration day ahead.

        Args:
            name:
            name_prefix:
            market_configuration:
            price_area:
            bid_date_specification:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of bid configuration edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_price_area: Whether to retrieve the price area
                for each benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the shop start specification
                for each benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the shop end specification
                for each benchmarking configuration day ahead or not.

        Returns:
            BidConfigurationDayAheadQueryAPI: The query API for the bid configuration day ahead.
        """
        from .bid_configuration_day_ahead_query import BidConfigurationDayAheadQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "benchmarkingBidConfigurations"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "bidConfigurations"),
            )
        )

        view_id = BidConfigurationDayAheadQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_bid_configuration_day_ahead_filter(
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
        return (BidConfigurationDayAheadQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))
    def assets_per_shop_model(
        self,
        shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> ShopModelWithAssetsQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the assets per shop model edges of the benchmarking configuration day ahead.

        Args:
            shop_model:
            shop_commands:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of assets per shop model edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_price_area: Whether to retrieve the price area
                for each benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the shop start specification
                for each benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the shop end specification
                for each benchmarking configuration day ahead or not.

        Returns:
            ShopModelWithAssetsQueryAPI: The query API for the shop model with asset.
        """
        from .shop_model_with_assets_query import ShopModelWithAssetsQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "assetsPerShopModel"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "assetsPerShopModel"),
            )
        )

        view_id = ShopModelWithAssetsQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_shop_model_with_asset_filter(
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
        return (ShopModelWithAssetsQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))

    def query(
        self,
        retrieve_price_area: bool = False,
        retrieve_shop_start_specification: bool = False,
        retrieve_shop_end_specification: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_price_area: Whether to retrieve the
                price area for each
                benchmarking configuration day ahead or not.
            retrieve_shop_start_specification: Whether to retrieve the
                shop start specification for each
                benchmarking configuration day ahead or not.
            retrieve_shop_end_specification: Whether to retrieve the
                shop end specification for each
                benchmarking configuration day ahead or not.

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
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("priceArea"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PriceAreaDayAhead._view_id]),
                ),
                view_id=PriceAreaDayAhead._view_id,
                connection_property=ViewPropertyId(self._view_id, "priceArea"),
            ),
        )
    def _query_append_shop_start_specification(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("shopStartSpecification"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                ),
                view_id=DateSpecification._view_id,
                connection_property=ViewPropertyId(self._view_id, "shopStartSpecification"),
            ),
        )
    def _query_append_shop_end_specification(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("shopEndSpecification"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                ),
                view_id=DateSpecification._view_id,
                connection_property=ViewPropertyId(self._view_id, "shopEndSpecification"),
            ),
        )
