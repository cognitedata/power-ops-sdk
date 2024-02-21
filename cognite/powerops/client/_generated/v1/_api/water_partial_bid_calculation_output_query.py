from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    WaterPartialBidCalculationOutput,
    BidMatrixRaw,
    WaterPartialBidCalculationInput,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI


class WaterPartialBidCalculationOutputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("water_partial_bid_calculation_output"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[WaterPartialBidCalculationOutput], ["*"])]
                ),
                result_cls=WaterPartialBidCalculationOutput,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_raw_partial_matrix: bool = False,
        retrieve_input_: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the water partial bid calculation output.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_raw_partial_matrix: Whether to retrieve the raw partial matrix for each water partial bid calculation output or not.
            retrieve_input_: Whether to retrieve the input for each water partial bid calculation output or not.

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
        if retrieve_raw_partial_matrix:
            self._query_append_raw_partial_matrix(from_)
        if retrieve_input_:
            self._query_append_input_(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_raw_partial_matrix: bool = False,
        retrieve_input_: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_raw_partial_matrix: Whether to retrieve the raw partial matrix for each water partial bid calculation output or not.
            retrieve_input_: Whether to retrieve the input for each water partial bid calculation output or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_raw_partial_matrix:
            self._query_append_raw_partial_matrix(from_)
        if retrieve_input_:
            self._query_append_input_(from_)
        return self._query()

    def _query_append_raw_partial_matrix(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMatrixRaw]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("raw_partial_matrix"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[WaterPartialBidCalculationOutput].as_property_ref(
                        "rawPartialMatrix"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMatrixRaw,
            ),
        )

    def _query_append_input_(self, from_: str) -> None:
        view_id = self._view_by_read_class[WaterPartialBidCalculationInput]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("input_"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[WaterPartialBidCalculationOutput].as_property_ref("input"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=WaterPartialBidCalculationInput,
            ),
        )
