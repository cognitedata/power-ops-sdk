from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidDocumentDayAheadPartialsAPI(EdgeAPI):
    def list(
            self,
            from_bid_document_day_ahead: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_bid_document_day_ahead_space: str = DEFAULT_INSTANCE_SPACE,
            to_partial_bid_matrix_information: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_partial_bid_matrix_information_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List partial edges of a bid document day ahead.

        Args:
            from_bid_document_day_ahead: ID of the source bid document day ahead.
            from_bid_document_day_ahead_space: Location of the bid document day aheads.
            to_partial_bid_matrix_information: ID of the target partial bid matrix information.
            to_partial_bid_matrix_information_space: Location of the partial bid matrix information.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial edges.

        Examples:

            List 5 partial edges connected to "my_bid_document_day_ahead":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_ahead = client.bid_document_day_ahead.partials_edge.list("my_bid_document_day_ahead", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "partialBid"),

            from_bid_document_day_ahead,
            from_bid_document_day_ahead_space,
            to_partial_bid_matrix_information,
            to_partial_bid_matrix_information_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
