from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BidDocumentDayAhead,
    PriceArea,
    BidMethodDayAhead,
    BidMatrix,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .bid_matrix_query import BidMatrixQueryAPI


class BidDocumentDayAheadQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("bid_document_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[BidDocumentDayAhead], ["*"])]),
                result_cls=BidDocumentDayAhead,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_method: bool = False,
        retrieve_total: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the bid document day ahead.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each bid document day ahead or not.
            retrieve_method: Whether to retrieve the method for each bid document day ahead or not.
            retrieve_total: Whether to retrieve the total for each bid document day ahead or not.

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
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_total:
            self._query_append_total(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def partials(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_method: bool = False,
        retrieve_total: bool = False,
    ) -> BidMatrixQueryAPI[T_DomainModelList]:
        """Query along the partial edges of the bid document day ahead.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each bid document day ahead or not.
            retrieve_method: Whether to retrieve the method for each bid document day ahead or not.
            retrieve_total: Whether to retrieve the total for each bid document day ahead or not.

        Returns:
            BidMatrixQueryAPI: The query API for the bid matrix.
        """
        from .bid_matrix_query import BidMatrixQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "partialBid"),
            external_id_prefix=external_id_prefix,
            space=space,
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
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_total:
            self._query_append_total(from_)
        return BidMatrixQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_price_area: bool = False,
        retrieve_method: bool = False,
        retrieve_total: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_price_area: Whether to retrieve the price area for each bid document day ahead or not.
            retrieve_method: Whether to retrieve the method for each bid document day ahead or not.
            retrieve_total: Whether to retrieve the total for each bid document day ahead or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_price_area:
            self._query_append_price_area(from_)
        if retrieve_method:
            self._query_append_method(from_)
        if retrieve_total:
            self._query_append_total(from_)
        return self._query()

    def _query_append_price_area(self, from_: str) -> None:
        view_id = self._view_by_read_class[PriceArea]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidDocumentDayAhead].as_property_ref("priceArea"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceArea,
            ),
        )

    def _query_append_method(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMethodDayAhead]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("method"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidDocumentDayAhead].as_property_ref("method"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethodDayAhead,
            ),
        )

    def _query_append_total(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMatrix]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("total"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidDocumentDayAhead].as_property_ref("total"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMatrix,
            ),
        )
