from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidRowAlertsAPI(EdgeAPI):
    def list(
        self,
        bid_row: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_row_space: str = "power-ops-afrr-bids",
        alert: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        alert_space: str = "power-ops-afrr-bids",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List alert edges of a bid row.

        Args:
            bid_row: ID of the source bid rows.
            bid_row_space: Location of the bid rows.
            alert: ID of the target alerts.
            alert_space: Location of the alerts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alert edges connected to "my_bid_row":

                >>> from cognite.powerops.client._generated.afrr_bids import AFRRBidsAPI
                >>> client = AFRRBidsAPI()
                >>> bid_row = client.bid_row.alerts_edge.list("my_bid_row", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-afrr-bids", "BidRow.alerts"),
            bid_row,
            bid_row_space,
            alert,
            alert_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
