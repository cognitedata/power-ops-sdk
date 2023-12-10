from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelApply,
    BidDocument,
    BidDocumentApply,
    PriceArea,
    PriceAreaApply,
    BidMethod,
    BidMethodApply,
    BidTable,
    BidTableApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .bid_table_query import BidTableQueryAPI


class BidDocumentQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_document"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[BidDocumentApply], ["*"])]),
                result_cls=BidDocument,
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
        """Query along the alert edges of the bid document.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each bid document or not.
            retrieve_method: Whether to retrieve the method for each bid document or not.
            retrieve_total: Whether to retrieve the total for each bid document or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "calculationIssue"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alerts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
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
        return AlertQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def partials(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_price_area: bool = False,
        retrieve_method: bool = False,
        retrieve_total: bool = False,
    ) -> BidTableQueryAPI[T_DomainModelList]:
        """Query along the partial edges of the bid document.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_price_area: Whether to retrieve the price area for each bid document or not.
            retrieve_method: Whether to retrieve the method for each bid document or not.
            retrieve_total: Whether to retrieve the total for each bid document or not.

        Returns:
            BidTableQueryAPI: The query API for the bid table.
        """
        from .bid_table_query import BidTableQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "PartialBid"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partials"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
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
        return BidTableQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_price_area: bool = False,
        retrieve_method: bool = False,
        retrieve_total: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_price_area: Whether to retrieve the price area for each bid document or not.
            retrieve_method: Whether to retrieve the method for each bid document or not.
            retrieve_total: Whether to retrieve the total for each bid document or not.

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
        view_id = self._view_by_write_class[PriceAreaApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[BidDocumentApply].as_property_ref("price_area"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceArea,
            ),
        )

    def _query_append_method(self, from_: str) -> None:
        view_id = self._view_by_write_class[BidMethodApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("method"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[BidDocumentApply].as_property_ref("method"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethod,
            ),
        )

    def _query_append_total(self, from_: str) -> None:
        view_id = self._view_by_write_class[BidTableApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("total"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[BidDocumentApply].as_property_ref("total"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidTable,
            ),
        )
