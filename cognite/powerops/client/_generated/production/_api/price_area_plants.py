from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.production.data_classes._core import DEFAULT_INSTANCE_SPACE


class PriceAreaPlantsAPI(EdgeAPI):
    def list(
        self,
        from_price_area: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_price_area_space: str = DEFAULT_INSTANCE_SPACE,
        to_plant: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_plant_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List plant edges of a price area.

        Args:
            from_price_area: ID of the source price area.
            from_price_area_space: Location of the price areas.
            to_plant: ID of the target plant.
            to_plant_space: Location of the plants.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested plant edges.

        Examples:

            List 5 plant edges connected to "my_price_area":

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> price_area = client.price_area.plants_edge.list("my_price_area", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops", "PriceArea.plants"),
            from_price_area,
            from_price_area_space,
            to_plant,
            to_plant_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
