from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class TotalBidMatrixCalculationInputPartialBidMatricesAPI(EdgeAPI):
    def list(
            self,
            from_total_bid_matrix_calculation_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_total_bid_matrix_calculation_input_space: str = DEFAULT_INSTANCE_SPACE,
            to_bid_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_bid_matrix_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List partial bid matrice edges of a total bid matrix calculation input.

        Args:
            from_total_bid_matrix_calculation_input: ID of the source total bid matrix calculation input.
            from_total_bid_matrix_calculation_input_space: Location of the total bid matrix calculation inputs.
            to_bid_matrix: ID of the target bid matrix.
            to_bid_matrix_space: Location of the bid matrixes.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrice edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested partial bid matrice edges.

        Examples:

            List 5 partial bid matrice edges connected to "my_total_bid_matrix_calculation_input":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_input = client.total_bid_matrix_calculation_input.partial_bid_matrices_edge.list("my_total_bid_matrix_calculation_input", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "BidMatrix"),

            from_total_bid_matrix_calculation_input,
            from_total_bid_matrix_calculation_input_space,
            to_bid_matrix,
            to_bid_matrix_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
