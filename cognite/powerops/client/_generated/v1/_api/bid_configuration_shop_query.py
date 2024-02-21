from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BidConfigurationShop,
    MarketConfiguration,
    BidMethodSHOPMultiScenario,
    PriceArea,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .plant_shop_query import PlantShopQueryAPI
    from .watercourse_shop_query import WatercourseShopQueryAPI


class BidConfigurationShopQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_configuration_shop"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[BidConfigurationShop], ["*"])]
                ),
                result_cls=BidConfigurationShop,
                max_retrieve_limit=limit,
            )
        )

    def plants_shop(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> PlantShopQueryAPI[T_DomainModelList]:
        """Query along the plants shop edges of the bid configuration shop.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants shop edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration shop or not.
            retrieve_method: Whether to retrieve the method for each bid configuration shop or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration shop or not.

        Returns:
            PlantShopQueryAPI: The query API for the plant shop.
        """
        from .plant_shop_query import PlantShopQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plantsShop"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plants_shop"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        return PlantShopQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def watercourses_shop(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> WatercourseShopQueryAPI[T_DomainModelList]:
        """Query along the watercourses shop edges of the bid configuration shop.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses shop edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration shop or not.
            retrieve_method: Whether to retrieve the method for each bid configuration shop or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration shop or not.

        Returns:
            WatercourseShopQueryAPI: The query API for the watercourse shop.
        """
        from .watercourse_shop_query import WatercourseShopQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercoursesShop"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourses_shop"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        return WatercourseShopQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration shop or not.
            retrieve_method: Whether to retrieve the method for each bid configuration shop or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration shop or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        return self._query()

    def _query_append_market_configuration(self, from_: str) -> None:
        view_id = self._view_by_read_class[MarketConfiguration]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("market_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidConfigurationShop].as_property_ref("marketConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MarketConfiguration,
            ),
        )

    def _query_append_method(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMethodSHOPMultiScenario]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("method"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidConfigurationShop].as_property_ref("method"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethodSHOPMultiScenario,
            ),
        )

    def _query_append_price_area(self, from_: str) -> None:
        view_id = self._view_by_read_class[PriceArea]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidConfigurationShop].as_property_ref("priceArea"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceArea,
            ),
        )
