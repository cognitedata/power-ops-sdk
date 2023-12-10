from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE


class GeneratorTurbineCurvesAPI(EdgeAPI):
    def list(
        self,
        from_generator: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_generator_space: str = DEFAULT_INSTANCE_SPACE,
        to_turbine_efficiency_curve: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_turbine_efficiency_curve_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List turbine curve edges of a generator.

        Args:
            from_generator: ID of the source generator.
            from_generator_space: Location of the generators.
            to_turbine_efficiency_curve: ID of the target turbine efficiency curve.
            to_turbine_efficiency_curve_space: Location of the turbine efficiency curves.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine curve edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested turbine curve edges.

        Examples:

            List 5 turbine curve edges connected to "my_generator":

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generator = client.generator.turbine_curves_edge.list("my_generator", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            from_generator,
            from_generator_space,
            to_turbine_efficiency_curve,
            to_turbine_efficiency_curve_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
