from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopScenarioSetScenariosAPI(EdgeAPI):
    def list(
            self,
            from_shop_scenario_set: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_scenario_set_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_scenario_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List scenario edges of a shop scenario set.

        Args:
            from_shop_scenario_set: ID of the source shop scenario set.
            from_shop_scenario_set_space: Location of the shop scenario sets.
            to_shop_scenario: ID of the target shop scenario.
            to_shop_scenario_space: Location of the shop scenarios.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested scenario edges.

        Examples:

            List 5 scenario edges connected to "my_shop_scenario_set":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario_set = client.shop_scenario_set.scenarios_edge.list("my_shop_scenario_set", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios"),

            from_shop_scenario_set,
            from_shop_scenario_set_space,
            to_shop_scenario,
            to_shop_scenario_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
