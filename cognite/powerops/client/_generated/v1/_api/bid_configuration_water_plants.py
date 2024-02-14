from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BidConfigurationWaterPlantsAPI(EdgeAPI):
    def list(
        self,
        from_bid_configuration_water: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_bid_configuration_water_space: str = DEFAULT_INSTANCE_SPACE,
        to_plant: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_plant_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List plant edges of a bid configuration water.

        Args:
            from_bid_configuration_water: ID of the source bid configuration water.
            from_bid_configuration_water_space: Location of the bid configuration waters.
            to_plant: ID of the target plant.
            to_plant_space: Location of the plants.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested plant edges.

        Examples:

            List 5 plant edges connected to "my_bid_configuration_water":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_water = client.bid_configuration_water.plants_edge.list("my_bid_configuration_water", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plants"),
            from_bid_configuration_water,
            from_bid_configuration_water_space,
            to_plant,
            to_plant_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
