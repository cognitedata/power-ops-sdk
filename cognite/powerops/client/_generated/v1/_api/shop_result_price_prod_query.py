from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    SHOPResultPriceProd,
    Case,
    SHOPTimeSeries,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .shop_time_series_query import SHOPTimeSeriesQueryAPI


class SHOPResultPriceProdQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("shop_result_price_prod"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[SHOPResultPriceProd], ["*"])]),
                result_cls=SHOPResultPriceProd,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_case: bool = False,
        retrieve_price_timeseries: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the shop result price prod.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_case: Whether to retrieve the case for each shop result price prod or not.
            retrieve_price_timeseries: Whether to retrieve the price timesery for each shop result price prod or not.

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
        if retrieve_case:
            self._query_append_case(from_)
        if retrieve_price_timeseries:
            self._query_append_price_timeseries(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def production_timeseries(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_case: bool = False,
        retrieve_price_timeseries: bool = False,
    ) -> SHOPTimeSeriesQueryAPI[T_DomainModelList]:
        """Query along the production timesery edges of the shop result price prod.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production timesery edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_case: Whether to retrieve the case for each shop result price prod or not.
            retrieve_price_timeseries: Whether to retrieve the price timesery for each shop result price prod or not.

        Returns:
            SHOPTimeSeriesQueryAPI: The query API for the shop time series.
        """
        from .shop_time_series_query import SHOPTimeSeriesQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "SHOPResultPriceProd.productionTimeseries"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("production_timeseries"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_case:
            self._query_append_case(from_)
        if retrieve_price_timeseries:
            self._query_append_price_timeseries(from_)
        return SHOPTimeSeriesQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_case: bool = False,
        retrieve_price_timeseries: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_case: Whether to retrieve the case for each shop result price prod or not.
            retrieve_price_timeseries: Whether to retrieve the price timesery for each shop result price prod or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_case:
            self._query_append_case(from_)
        if retrieve_price_timeseries:
            self._query_append_price_timeseries(from_)
        return self._query()

    def _query_append_case(self, from_: str) -> None:
        view_id = self._view_by_read_class[Case]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("case"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[SHOPResultPriceProd].as_property_ref("case"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Case,
            ),
        )

    def _query_append_price_timeseries(self, from_: str) -> None:
        view_id = self._view_by_read_class[SHOPTimeSeries]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_timeseries"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[SHOPResultPriceProd].as_property_ref("priceTimeseries"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=SHOPTimeSeries,
            ),
        )
