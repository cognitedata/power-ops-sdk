from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE


class PlantGeneratorsAPI(EdgeAPI):
    def list(
        self,
        from_plant: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_plant_space: str = DEFAULT_INSTANCE_SPACE,
        to_generator: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_generator_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List generator edges of a plant.

        Args:
            from_plant: ID of the source plant.
            from_plant_space: Location of the plants.
            to_generator: ID of the target generator.
            to_generator_space: Location of the generators.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested generator edges.

        Examples:

            List 5 generator edges connected to "my_plant":

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> plant = client.plant.generators_edge.list("my_plant", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            from_plant,
            from_plant_space,
            to_generator,
            to_generator_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
