from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidDocumentPartialsAPI(EdgeAPI):
    def list(
        self,
        bid_document: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_document_space: str = "power-ops-day-ahead-bids",
        bid_table: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_table_space: str = "power-ops-day-ahead-bids",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List partial edges of a bid document.

        Args:
            bid_document: ID of the source bid documents.
            bid_document_space: Location of the bid documents.
            bid_table: ID of the target bid tables.
            bid_table_space: Location of the bid tables.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial edges.

        Examples:

            List 5 partial edges connected to "my_bid_document":

                >>> from cognite.powerops.client._generated.day_ahead_bids import DayAheadBidsAPI
                >>> client = DayAheadBidsAPI()
                >>> bid_document = client.bid_document.partials_edge.list("my_bid_document", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-day-ahead-bids", "Bid.partials"),
            bid_document,
            bid_document_space,
            bid_table,
            bid_table_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
