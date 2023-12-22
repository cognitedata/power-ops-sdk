from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE


class WatercoursePlantsAPI(EdgeAPI):
    def list(
        self,
        from_watercourse: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_watercourse_space: str = DEFAULT_INSTANCE_SPACE,
        to_plant: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_plant_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List plant edges of a watercourse.

        Args:
            from_watercourse: ID of the source watercourse.
            from_watercourse_space: Location of the watercourses.
            to_plant: ID of the target plant.
            to_plant_space: Location of the plants.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested plant edges.

        Examples:

            List 5 plant edges connected to "my_watercourse":

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> watercourse = client.watercourse.plants_edge.list("my_watercourse", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            from_watercourse,
            from_watercourse_space,
            to_plant,
            to_plant_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
