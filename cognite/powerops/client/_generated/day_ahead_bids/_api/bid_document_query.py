from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from cognite.powerops.client._generated.day_ahead_bids.data_classes import (
    BidDocument,
    BidDocumentApply,
    Alert,
    AlertApply,
    BidTable,
    BidTableApply,
)
from cognite.powerops.client._generated.day_ahead_bids.data_classes._bid_document import (
    _BIDDOCUMENT_PROPERTIES_BY_FIELD,
)
from cognite.powerops.client._generated.day_ahead_bids.data_classes._alert import (
    _ALERT_PROPERTIES_BY_FIELD,
)
from cognite.powerops.client._generated.day_ahead_bids.data_classes._bid_table import (
    _BIDTABLE_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .bid_table_query import BidTableQueryAPI


class BidDocumentQueryAPI(QueryAPI[T_DomainModelList]):
    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the bid document.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-day-ahead-bids", "Bid.alerts"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alerts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alert"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[AlertApply],
                            list(_ALERT_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Alert,
                max_retrieve_limit=-1,
            ),
        )
        return AlertQueryAPI(self._client, self._builder, self._view_by_write_class)

    def partials(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> BidTableQueryAPI[T_DomainModelList]:
        """Query along the partial edges of the bid document.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            BidTableQueryAPI: The query API for the bid table.
        """
        from .bid_table_query import BidTableQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-day-ahead-bids", "Bid.partials"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partials"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_table"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[BidTableApply],
                            list(_BIDTABLE_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=BidTable,
                max_retrieve_limit=-1,
            ),
        )
        return BidTableQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_bid_document: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_document: Whether to retrieve the bid document or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_document and not self._builder[-1].name.startswith("bid_document"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("bid_document"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BidDocumentApply],
                                list(_BIDDOCUMENT_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BidDocument,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
