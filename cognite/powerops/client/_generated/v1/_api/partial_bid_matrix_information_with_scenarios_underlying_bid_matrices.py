from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI(EdgeAPI):
    def list(
        self,
        from_partial_bid_matrix_information_with_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_partial_bid_matrix_information_with_scenario_space: str = DEFAULT_INSTANCE_SPACE,
        to_bid_matrix: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_bid_matrix_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List underlying bid matrice edges of a partial bid matrix information with scenario.

        Args:
            from_partial_bid_matrix_information_with_scenario: ID of the source partial bid matrix information with scenario.
            from_partial_bid_matrix_information_with_scenario_space: Location of the partial bid matrix information with scenarios.
            to_bid_matrix: ID of the target bid matrix.
            to_bid_matrix_space: Location of the bid matrixes.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of underlying bid matrice edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested underlying bid matrice edges.

        Examples:

            List 5 underlying bid matrice edges connected to "my_partial_bid_matrix_information_with_scenario":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenario = client.partial_bid_matrix_information_with_scenarios.underlying_bid_matrices_edge.list("my_partial_bid_matrix_information_with_scenario", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),
            from_partial_bid_matrix_information_with_scenario,
            from_partial_bid_matrix_information_with_scenario_space,
            to_bid_matrix,
            to_bid_matrix_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
