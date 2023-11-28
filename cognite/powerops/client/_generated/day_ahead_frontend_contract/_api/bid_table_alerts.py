from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidTableAlertsAPI(EdgeAPI):
    def list(
        self,
        bid_table: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_table_space: str = "dayAheadFrontendContractModel",
        alert: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        alert_space: str = "dayAheadFrontendContractModel",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List alert edges of a bid table.

        Args:
            bid_table: ID of the source bid tables.
            bid_table_space: Location of the bid tables.
            alert: ID of the target alerts.
            alert_space: Location of the alerts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alert edges connected to "my_bid_table":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid_table = client.bid_table.alerts_edge.list("my_bid_table", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("dayAheadFrontendContractModel", "BidTable.alerts"),
            bid_table,
            bid_table_space,
            alert,
            alert_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
