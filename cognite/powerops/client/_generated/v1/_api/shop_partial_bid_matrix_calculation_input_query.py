from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopPartialBidMatrixCalculationInput,
    BidConfiguration,
    ShopBasedPartialBidConfiguration,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .price_production_query import PriceProductionQueryAPI


class ShopPartialBidMatrixCalculationInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("shop_partial_bid_matrix_calculation_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[ShopPartialBidMatrixCalculationInput], ["*"])]
                ),
                result_cls=ShopPartialBidMatrixCalculationInput,
                max_retrieve_limit=limit,
            )
        )

    def price_production(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_bid_configuration: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> PriceProductionQueryAPI[T_DomainModelList]:
        """Query along the price production edges of the shop partial bid matrix calculation input.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price production edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each shop partial bid matrix calculation input or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each shop partial bid matrix calculation input or not.

        Returns:
            PriceProductionQueryAPI: The query API for the price production.
        """
        from .price_production_query import PriceProductionQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types_temp", "PriceProduction"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_production"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return PriceProductionQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_bid_configuration: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each shop partial bid matrix calculation input or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each shop partial bid matrix calculation input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return self._query()

    def _query_append_bid_configuration(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidConfiguration]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopPartialBidMatrixCalculationInput].as_property_ref(
                        "bidConfiguration"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidConfiguration,
            ),
        )

    def _query_append_partial_bid_configuration(self, from_: str) -> None:
        view_id = self._view_by_read_class[ShopBasedPartialBidConfiguration]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partial_bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopPartialBidMatrixCalculationInput].as_property_ref(
                        "partialBidConfiguration"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopBasedPartialBidConfiguration,
            ),
        )
