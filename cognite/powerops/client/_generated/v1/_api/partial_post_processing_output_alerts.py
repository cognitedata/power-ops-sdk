from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class PartialPostProcessingOutputAlertsAPI(EdgeAPI):
    def list(
        self,
        from_partial_post_processing_output: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_partial_post_processing_output_space: str = DEFAULT_INSTANCE_SPACE,
        to_alert: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_alert_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List alert edges of a partial post processing output.

        Args:
            from_partial_post_processing_output: ID of the source partial post processing output.
            from_partial_post_processing_output_space: Location of the partial post processing outputs.
            to_alert: ID of the target alert.
            to_alert_space: Location of the alerts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alert edges connected to "my_partial_post_processing_output":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_post_processing_output = client.partial_post_processing_output.alerts_edge.list("my_partial_post_processing_output", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
            from_partial_post_processing_output,
            from_partial_post_processing_output_space,
            to_alert,
            to_alert_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
