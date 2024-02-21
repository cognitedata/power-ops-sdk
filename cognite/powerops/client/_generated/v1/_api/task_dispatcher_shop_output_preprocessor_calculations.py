from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class TaskDispatcherShopOutputPreprocessorCalculationsAPI(EdgeAPI):
    def list(
        self,
        from_task_dispatcher_shop_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_task_dispatcher_shop_output_space: str = DEFAULT_INSTANCE_SPACE,
        to_preprocessor_input: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_preprocessor_input_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List preprocessor calculation edges of a task dispatcher shop output.

        Args:
            from_task_dispatcher_shop_output: ID of the source task dispatcher shop output.
            from_task_dispatcher_shop_output_space: Location of the task dispatcher shop outputs.
            to_preprocessor_input: ID of the target preprocessor input.
            to_preprocessor_input_space: Location of the preprocessor inputs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of preprocessor calculation edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested preprocessor calculation edges.

        Examples:

            List 5 preprocessor calculation edges connected to "my_task_dispatcher_shop_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_shop_output = client.task_dispatcher_shop_output.preprocessor_calculations_edge.list("my_task_dispatcher_shop_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "PreprocessorInput"),
            from_task_dispatcher_shop_output,
            from_task_dispatcher_shop_output_space,
            to_preprocessor_input,
            to_preprocessor_input_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
