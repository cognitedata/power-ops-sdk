from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidAlertsAPI(EdgeAPI):
    def list(
        self,
        bid: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_space: str = "poweropsDayAheadFrontendContractModel",
        alert: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        alert_space: str = "poweropsDayAheadFrontendContractModel",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List alert edges of a bid.

        Args:
            bid: ID of the source bids.
            bid_space: Location of the bids.
            alert: ID of the target alerts.
            alert_space: Location of the alerts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alert edges connected to "my_bid":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.alerts_edge.list("my_bid", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("poweropsDayAheadFrontendContractModel", "Bid.alerts"),
            bid,
            bid_space,
            alert,
            alert_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
