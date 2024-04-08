from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ScenarioSetShopScenariosAPI(EdgeAPI):
    def list(
        self,
        from_scenario_set: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_scenario_set_space: str = DEFAULT_INSTANCE_SPACE,
        to_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_scenario_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List shop scenario edges of a scenario set.

        Args:
            from_scenario_set: ID of the source scenario set.
            from_scenario_set_space: Location of the scenario sets.
            to_scenario: ID of the target scenario.
            to_scenario_space: Location of the scenarios.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested shop scenario edges.

        Examples:

            List 5 shop scenario edges connected to "my_scenario_set":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenario_set = client.scenario_set.shop_scenarios_edge.list("my_scenario_set", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types_temp", "ScenarioSet.scenarios"),
            from_scenario_set,
            from_scenario_set_space,
            to_scenario,
            to_scenario_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
