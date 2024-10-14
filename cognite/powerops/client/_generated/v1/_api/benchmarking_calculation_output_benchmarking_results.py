from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BenchmarkingCalculationOutputBenchmarkingResultsAPI(EdgeAPI):
    def list(
            self,
            from_benchmarking_calculation_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_benchmarking_calculation_output_space: str = DEFAULT_INSTANCE_SPACE,
            to_benchmarking_result_day_ahead: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_benchmarking_result_day_ahead_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List benchmarking result edges of a benchmarking calculation output.

        Args:
            from_benchmarking_calculation_output: ID of the source benchmarking calculation output.
            from_benchmarking_calculation_output_space: Location of the benchmarking calculation outputs.
            to_benchmarking_result_day_ahead: ID of the target benchmarking result day ahead.
            to_benchmarking_result_day_ahead_space: Location of the benchmarking result day aheads.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking result edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested benchmarking result edges.

        Examples:

            List 5 benchmarking result edges connected to "my_benchmarking_calculation_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_calculation_output = client.benchmarking_calculation_output.benchmarking_results_edge.list("my_benchmarking_calculation_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "BenchmarkingResultsDayAhead"),

            from_benchmarking_calculation_output,
            from_benchmarking_calculation_output_space,
            to_benchmarking_result_day_ahead,
            to_benchmarking_result_day_ahead_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
