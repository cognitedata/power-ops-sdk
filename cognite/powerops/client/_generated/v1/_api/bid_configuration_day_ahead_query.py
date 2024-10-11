from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BidConfigurationDayAhead,
    MarketConfiguration,
    PriceAreaDayAhead,
    DateSpecification,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import (
    PartialBidConfiguration,
    _create_partial_bid_configuration_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .partial_bid_configuration_query import PartialBidConfigurationQueryAPI



class BidConfigurationDayAheadQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")

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
                name=self._builder.next_name("bid_configuration_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=BidConfigurationDayAhead,
                max_retrieve_limit=limit,
            )
        )

    def partials(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_market_configuration: bool = False,
        retrieve_price_area: bool = False,
        retrieve_bid_date_specification: bool = False,
    ) -> PartialBidConfigurationQueryAPI[T_DomainModelList]:
        """Query along the partial edges of the bid configuration day ahead.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of partial edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration day ahead or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration day ahead or not.
            retrieve_bid_date_specification: Whether to retrieve the bid date specification for each bid configuration day ahead or not.

        Returns:
            PartialBidConfigurationQueryAPI: The query API for the partial bid configuration.
        """
        from .partial_bid_configuration_query import PartialBidConfigurationQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partials"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = PartialBidConfigurationQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_partial_bid_configuration_filter(
            view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_bid_date_specification:
            self._query_append_bid_date_specification(from_)
        return PartialBidConfigurationQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_market_configuration: bool = False,
        retrieve_price_area: bool = False,
        retrieve_bid_date_specification: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market_configuration: Whether to retrieve the market configuration for each bid configuration day ahead or not.
            retrieve_price_area: Whether to retrieve the price area for each bid configuration day ahead or not.
            retrieve_bid_date_specification: Whether to retrieve the bid date specification for each bid configuration day ahead or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market_configuration:
            self._query_append_market_configuration(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_bid_date_specification:
            self._query_append_bid_date_specification(from_)
        return self._query()

    def _query_append_market_configuration(self, from_: str) -> None:
        view_id = MarketConfiguration._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("market_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("marketConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MarketConfiguration,
                is_single_direct_relation=True,
            ),
        )

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

    def _query_append_bid_date_specification(self, from_: str) -> None:
        view_id = DateSpecification._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_date_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("bidDateSpecification"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateSpecification,
                is_single_direct_relation=True,
            ),
        )
