from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

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
    from cognite.powerops.client._generated.v1._api.partial_bid_configuration_query import PartialBidConfigurationQueryAPI


class BidConfigurationDayAheadQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials"),
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
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("marketConfiguration"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[MarketConfiguration._view_id]),
                ),
                result_cls=MarketConfiguration,
            ),
        )
    def _query_append_price_area(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("priceArea"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PriceAreaDayAhead._view_id]),
                ),
                result_cls=PriceAreaDayAhead,
            ),
        )
    def _query_append_bid_date_specification(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bidDateSpecification"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                ),
                result_cls=DateSpecification,
            ),
        )
