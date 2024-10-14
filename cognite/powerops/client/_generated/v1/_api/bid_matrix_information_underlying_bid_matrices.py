from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidMatrixInformationUnderlyingBidMatricesAPI(EdgeAPI):
    def list(
            self,
            from_bid_matrix_information: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_bid_matrix_information_space: str = DEFAULT_INSTANCE_SPACE,
            to_bid_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_bid_matrix_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List underlying bid matrice edges of a bid matrix information.

        Args:
            from_bid_matrix_information: ID of the source bid matrix information.
            from_bid_matrix_information_space: Location of the bid matrix information.
            to_bid_matrix: ID of the target bid matrix.
            to_bid_matrix_space: Location of the bid matrixes.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of underlying bid matrice edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested underlying bid matrice edges.

        Examples:

            List 5 underlying bid matrice edges connected to "my_bid_matrix_information":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrix_information = client.bid_matrix_information.underlying_bid_matrices_edge.list("my_bid_matrix_information", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),

            from_bid_matrix_information,
            from_bid_matrix_information_space,
            to_bid_matrix,
            to_bid_matrix_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
