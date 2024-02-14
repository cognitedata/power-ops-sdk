from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class PartialPostProcessingOutputPartialMatricesAPI(EdgeAPI):
    def list(
        self,
        from_partial_post_processing_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_partial_post_processing_output_space: str = DEFAULT_INSTANCE_SPACE,
        to_bid_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_bid_matrix_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List partial matrice edges of a partial post processing output.

        Args:
            from_partial_post_processing_output: ID of the source partial post processing output.
            from_partial_post_processing_output_space: Location of the partial post processing outputs.
            to_bid_matrix: ID of the target bid matrix.
            to_bid_matrix_space: Location of the bid matrixes.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial matrice edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial matrice edges.

        Examples:

            List 5 partial matrice edges connected to "my_partial_post_processing_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_post_processing_output = client.partial_post_processing_output.partial_matrices_edge.list("my_partial_post_processing_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidMatrix"),
            from_partial_post_processing_output,
            from_partial_post_processing_output_space,
            to_bid_matrix,
            to_bid_matrix_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
