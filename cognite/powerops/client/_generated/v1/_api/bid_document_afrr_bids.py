from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidDocumentAFRRBidsAPI(EdgeAPI):
    def list(
            self,
            from_bid_document_afrr: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_bid_document_afrr_space: str = DEFAULT_INSTANCE_SPACE,
            to_bid_row: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_bid_row_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List bid edges of a bid document afrr.

        Args:
            from_bid_document_afrr: ID of the source bid document afrr.
            from_bid_document_afrr_space: Location of the bid document afrrs.
            to_bid_row: ID of the target bid row.
            to_bid_row_space: Location of the bid rows.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested bid edges.

        Examples:

            List 5 bid edges connected to "my_bid_document_afrr":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_afrr = client.bid_document_afrr.bids_edge.list("my_bid_document_afrr", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "partialBid"),

            from_bid_document_afrr,
            from_bid_document_afrr_space,
            to_bid_row,
            to_bid_row_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
