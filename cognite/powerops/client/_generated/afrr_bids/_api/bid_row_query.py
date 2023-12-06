from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from cognite.powerops.client._generated.afrr_bids.data_classes import (
    BidRow,
    BidRowApply,
    Alert,
    AlertApply,
)
from cognite.powerops.client._generated.afrr_bids.data_classes._bid_row import (
    _BIDROW_PROPERTIES_BY_FIELD,
)
from cognite.powerops.client._generated.afrr_bids.data_classes._alert import (
    _ALERT_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI


class BidRowQueryAPI(QueryAPI[T_DomainModelList]):
    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the bid row.

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
            dm.DirectRelationReference("power-ops-afrr-bids", "BidRow.alerts"),
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

    def query(
        self,
        retrieve_bid_row: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_row: Whether to retrieve the bid row or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_row and not self._builder[-1].name.startswith("bid_row"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("bid_row"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BidRowApply],
                                list(_BIDROW_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BidRow,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
