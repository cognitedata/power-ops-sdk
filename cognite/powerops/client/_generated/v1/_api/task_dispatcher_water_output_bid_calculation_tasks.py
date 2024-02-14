from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class TaskDispatcherWaterOutputBidCalculationTasksAPI(EdgeAPI):
    def list(
        self,
        from_task_dispatcher_water_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_task_dispatcher_water_output_space: str = DEFAULT_INSTANCE_SPACE,
        to_water_partial_bid_calculation_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_water_partial_bid_calculation_input_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List bid calculation task edges of a task dispatcher water output.

        Args:
            from_task_dispatcher_water_output: ID of the source task dispatcher water output.
            from_task_dispatcher_water_output_space: Location of the task dispatcher water outputs.
            to_water_partial_bid_calculation_input: ID of the target water partial bid calculation input.
            to_water_partial_bid_calculation_input_space: Location of the water partial bid calculation inputs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid calculation task edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested bid calculation task edges.

        Examples:

            List 5 bid calculation task edges connected to "my_task_dispatcher_water_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_water_output = client.task_dispatcher_water_output.bid_calculation_tasks_edge.list("my_task_dispatcher_water_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "Water.partialBidCalculations"),
            from_task_dispatcher_water_output,
            from_task_dispatcher_water_output_space,
            to_water_partial_bid_calculation_input,
            to_water_partial_bid_calculation_input_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
