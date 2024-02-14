from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class PartialPostProcessingInputPartialBidMatricesRawAPI(EdgeAPI):
    def list(
        self,
        from_partial_post_processing_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_partial_post_processing_input_space: str = DEFAULT_INSTANCE_SPACE,
        to_bid_matrix_raw: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_bid_matrix_raw_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List partial bid matrices raw edges of a partial post processing input.

        Args:
            from_partial_post_processing_input: ID of the source partial post processing input.
            from_partial_post_processing_input_space: Location of the partial post processing inputs.
            to_bid_matrix_raw: ID of the target bid matrix raw.
            to_bid_matrix_raw_space: Location of the bid matrix raws.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrices raw edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial bid matrices raw edges.

        Examples:

            List 5 partial bid matrices raw edges connected to "my_partial_post_processing_input":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_post_processing_input = client.partial_post_processing_input.partial_bid_matrices_raw_edge.list("my_partial_post_processing_input", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "partialBidMatricesRaw"),
            from_partial_post_processing_input,
            from_partial_post_processing_input_space,
            to_bid_matrix_raw,
            to_bid_matrix_raw_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
