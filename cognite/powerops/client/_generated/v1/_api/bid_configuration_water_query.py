from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BidConfigurationWater,
    MarketConfiguration,
    BidMethodWaterValue,
    PriceArea,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .plant_query import PlantQueryAPI
    from .watercourse_query import WatercourseQueryAPI


class BidConfigurationWaterQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("bid_configuration_water"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[BidConfigurationWater], ["*"])]
                ),
                result_cls=BidConfigurationWater,
                max_retrieve_limit=limit,
            )
        )

    def plants(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> PlantQueryAPI[T_DomainModelList]:
        """Query along the plant edges of the bid configuration water.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration water or not.
            retrieve_method: Whether to retrieve the method for each bid configuration water or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration water or not.

        Returns:
            PlantQueryAPI: The query API for the plant.
        """
        from .plant_query import PlantQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plants"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plants"),
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
        return PlantQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def watercourses(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> WatercourseQueryAPI[T_DomainModelList]:
        """Query along the watercourse edges of the bid configuration water.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration water or not.
            retrieve_method: Whether to retrieve the method for each bid configuration water or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration water or not.

        Returns:
            WatercourseQueryAPI: The query API for the watercourse.
        """
        from .watercourse_query import WatercourseQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercourses"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourses"),
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
        return WatercourseQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_market_configuration: bool = False,
        retrieve_method: bool = False,
        retrieve_price_area: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration water or not.
            retrieve_method: Whether to retrieve the method for each bid configuration water or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration water or not.

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
                    through=self._view_by_read_class[BidConfigurationWater].as_property_ref("marketConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MarketConfiguration,
            ),
        )

    def _query_append_method(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMethodWaterValue]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("method"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidConfigurationWater].as_property_ref("method"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethodWaterValue,
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
                    through=self._view_by_read_class[BidConfigurationWater].as_property_ref("priceArea"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceArea,
            ),
        )
