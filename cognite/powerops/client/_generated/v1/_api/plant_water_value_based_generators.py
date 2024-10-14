from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class PlantWaterValueBasedGeneratorsAPI(EdgeAPI):
    def list(
            self,
            from_plant_water_value_based: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_plant_water_value_based_space: str = DEFAULT_INSTANCE_SPACE,
            to_generator: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_generator_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List generator edges of a plant water value based.

        Args:
            from_plant_water_value_based: ID of the source plant water value based.
            from_plant_water_value_based_space: Location of the plant water value baseds.
            to_generator: ID of the target generator.
            to_generator_space: Location of the generators.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested generator edges.

        Examples:

            List 5 generator edges connected to "my_plant_water_value_based":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_based = client.plant_water_value_based.generators_edge.list("my_plant_water_value_based", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "isSubAssetOf"),

            from_plant_water_value_based,
            from_plant_water_value_based_space,
            to_generator,
            to_generator_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
