from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class BidDocumentBidsAPI(EdgeAPI):
    def list(
        self,
        bid_document: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_document_space: str = "power-ops-afrr-bids",
        bid_row: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        bid_row_space: str = "power-ops-afrr-bids",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List bid edges of a bid document.

        Args:
            bid_document: ID of the source bid documents.
            bid_document_space: Location of the bid documents.
            bid_row: ID of the target bid rows.
            bid_row_space: Location of the bid rows.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested bid edges.

        Examples:

            List 5 bid edges connected to "my_bid_document":

                >>> from cognite.powerops.client._generated.afrr_bids import AFRRBidsAPI
                >>> client = AFRRBidsAPI()
                >>> bid_document = client.bid_document.bids_edge.list("my_bid_document", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-afrr-bids", "BidDocument.bids"),
            bid_document,
            bid_document_space,
            bid_row,
            bid_row_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
