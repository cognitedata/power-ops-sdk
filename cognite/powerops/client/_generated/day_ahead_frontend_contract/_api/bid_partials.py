from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidPartialsAPI(EdgeAPI):
    def list(
        self,
        bid: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_space: str = "dayAheadFrontendContractModel",
        bid_table: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_table_space: str = "dayAheadFrontendContractModel",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List partial edges of a bid.

        Args:
            bid: ID of the source bids.
            bid_space: Location of the bids.
            bid_table: ID of the target bid tables.
            bid_table_space: Location of the bid tables.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial edges.

        Examples:

            List 5 partial edges connected to "my_bid":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.partials_edge.list("my_bid", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("dayAheadFrontendContractModel", "Bid.partials"),
            bid,
            bid_space,
            bid_table,
            bid_table_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
