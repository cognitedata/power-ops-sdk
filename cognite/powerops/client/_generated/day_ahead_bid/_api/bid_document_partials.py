from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidDocumentPartialsAPI(EdgeAPI):
    def list(
        self,
        from_bid_document: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_bid_document_space: str = DEFAULT_INSTANCE_SPACE,
        to_bid_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_bid_matrix_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List partial edges of a bid document.

        Args:
            from_bid_document: ID of the source bid document.
            from_bid_document_space: Location of the bid documents.
            to_bid_matrix: ID of the target bid matrix.
            to_bid_matrix_space: Location of the bid matrixes.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial edges.

        Examples:

            List 5 partial edges connected to "my_bid_document":

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_document = client.bid_document.partials_edge.list("my_bid_document", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "partialBid"),
            from_bid_document,
            from_bid_document_space,
            to_bid_matrix,
            to_bid_matrix_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
