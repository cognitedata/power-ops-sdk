from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class TaskDispatcherOutputProcessSubTasksAPI(EdgeAPI):
    def list(
            self,
            from_task_dispatcher_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_task_dispatcher_output_space: str = DEFAULT_INSTANCE_SPACE,
            to_function_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_function_input_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List process sub task edges of a task dispatcher output.

        Args:
            from_task_dispatcher_output: ID of the source task dispatcher output.
            from_task_dispatcher_output_space: Location of the task dispatcher outputs.
            to_function_input: ID of the target function input.
            to_function_input_space: Location of the function inputs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process sub task edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested process sub task edges.

        Examples:

            List 5 process sub task edges connected to "my_task_dispatcher_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_output = client.task_dispatcher_output.process_sub_tasks_edge.list("my_task_dispatcher_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "processSubTasks"),

            from_task_dispatcher_output,
            from_task_dispatcher_output_space,
            to_function_input,
            to_function_input_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
