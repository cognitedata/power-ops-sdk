from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    PartialPostProcessingInput,
    MarketConfiguration,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .bid_matrix_raw_query import BidMatrixRawQueryAPI


class PartialPostProcessingInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("partial_post_processing_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[PartialPostProcessingInput], ["*"])]
                ),
                result_cls=PartialPostProcessingInput,
                max_retrieve_limit=limit,
            )
        )

    def partial_bid_matrices_raw(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_market_config: bool = False,
    ) -> BidMatrixRawQueryAPI[T_DomainModelList]:
        """Query along the partial bid matrices raw edges of the partial post processing input.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrices raw edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_market_config: Whether to retrieve the market config for each partial post processing input or not.

        Returns:
            BidMatrixRawQueryAPI: The query API for the bid matrix raw.
        """
        from .bid_matrix_raw_query import BidMatrixRawQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "partialBidMatricesRaw"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partial_bid_matrices_raw"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_market_config:
            self._query_append_market_config(from_)
        return BidMatrixRawQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_market_config: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market_config: Whether to retrieve the market config for each partial post processing input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market_config:
            self._query_append_market_config(from_)
        return self._query()

    def _query_append_market_config(self, from_: str) -> None:
        view_id = self._view_by_read_class[MarketConfiguration]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("market_config"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[PartialPostProcessingInput].as_property_ref("marketConfig"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MarketConfiguration,
            ),
        )
