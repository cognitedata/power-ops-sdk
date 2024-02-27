from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopPartialBidCalculationInput,
    PlantShop,
    MarketConfiguration,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .shop_result_query import SHOPResultQueryAPI


class ShopPartialBidCalculationInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("shop_partial_bid_calculation_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[ShopPartialBidCalculationInput], ["*"])]
                ),
                result_cls=ShopPartialBidCalculationInput,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_plant: bool = False,
        retrieve_market_configuration: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the shop partial bid calculation input.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_plant: Whether to retrieve the plant for each shop partial bid calculation input or not.
            retrieve_market_configuration: Whether to retrieve the market configuration for each shop partial bid calculation input or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alerts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_plant:
            self._query_append_plant(from_)
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def shop_results(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_plant: bool = False,
        retrieve_market_configuration: bool = False,
    ) -> SHOPResultQueryAPI[T_DomainModelList]:
        """Query along the shop result edges of the shop partial bid calculation input.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_plant: Whether to retrieve the plant for each shop partial bid calculation input or not.
            retrieve_market_configuration: Whether to retrieve the market configuration for each shop partial bid calculation input or not.

        Returns:
            SHOPResultQueryAPI: The query API for the shop result.
        """
        from .shop_result_query import SHOPResultQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "SHOPResult"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_results"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_plant:
            self._query_append_plant(from_)
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        return SHOPResultQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_plant: bool = False,
        retrieve_market_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_plant: Whether to retrieve the plant for each shop partial bid calculation input or not.
            retrieve_market_configuration: Whether to retrieve the market configuration for each shop partial bid calculation input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_plant:
            self._query_append_plant(from_)
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        return self._query()

    def _query_append_plant(self, from_: str) -> None:
        view_id = self._view_by_read_class[PlantShop]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plant"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopPartialBidCalculationInput].as_property_ref("plant"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PlantShop,
            ),
        )

    def _query_append_market_configuration(self, from_: str) -> None:
        view_id = self._view_by_read_class[MarketConfiguration]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("market_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopPartialBidCalculationInput].as_property_ref(
                        "marketConfiguration"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MarketConfiguration,
            ),
        )
