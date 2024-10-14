from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ShopScenarioAttributeMappingsOverrideAPI(EdgeAPI):
    def list(
            self,
            from_shop_scenario: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_shop_scenario_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_attribute_mapping: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_attribute_mapping_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List attribute mappings override edges of a shop scenario.

        Args:
            from_shop_scenario: ID of the source shop scenario.
            from_shop_scenario_space: Location of the shop scenarios.
            to_shop_attribute_mapping: ID of the target shop attribute mapping.
            to_shop_attribute_mapping_space: Location of the shop attribute mappings.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of attribute mappings override edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested attribute mappings override edges.

        Examples:

            List 5 attribute mappings override edges connected to "my_shop_scenario":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario = client.shop_scenario.attribute_mappings_override_edge.list("my_shop_scenario", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping"),

            from_shop_scenario,
            from_shop_scenario_space,
            to_shop_attribute_mapping,
            to_shop_attribute_mapping_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
